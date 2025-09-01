import { Client as PgClient } from "pg";
import Cursor from "pg-cursor";
import "dotenv/config";
import { Client as EsClient } from "@elastic/elasticsearch";
import * as fs from "fs";
import * as path from "path";

const pg = new PgClient({
    host: "host",
    port: 5432,
    user: "postgres",
    password: "password",
    database: "db",
});

const es = new EsClient({
    node: "node",
    requestTimeout: 60 * 60 * 1000,
    pingTimeout: 60 * 60 * 1000,
    auth: {
        username: "username",
        password: "password",
    },
    tls: {
        rejectUnauthorized: false,
    },
});

type ScoreDetailOut = {
    categoryId: string;
    categorySlug?: string;
    categoryName?: string;
    score: number;
    justification?: string;
    phrases?: string[];
};

const PROGRESS_FILE =
    process.env.PROGRESS_FILE ||
    path.resolve(process.cwd(), "scoredetails_progress.json");
const FAILED_FILE =
    process.env.FAILED_FILE ||
    path.resolve(process.cwd(), "scoredetails_failed.jsonl");

function loadProgress(): Set<string> {
    try {
        if (!fs.existsSync(PROGRESS_FILE)) return new Set();
        const raw = fs.readFileSync(PROGRESS_FILE, "utf8").trim();
        if (!raw) return new Set();
        const arr = JSON.parse(raw);
        if (!Array.isArray(arr)) return new Set();
        return new Set(arr.map(String));
    } catch (e) {
        console.warn(`Could not read progress file; starting fresh (${PROGRESS_FILE})`, e);
        return new Set();
    }
}

function saveProgress(processed: Set<string>) {
    const tmp = PROGRESS_FILE + ".tmp";
    fs.writeFileSync(tmp, JSON.stringify(Array.from(processed)), "utf8");
    fs.renameSync(tmp, PROGRESS_FILE);
}

function appendFailed(ids: string[], reason?: string) {
    if (!ids.length) return;
    const ts = new Date().toISOString();
    const lines =
        ids.map((id) => JSON.stringify({ id, reason, ts })).join("\n") + "\n";
    fs.appendFileSync(FAILED_FILE, lines, "utf8");
}

const BULK_FLUSH_SIZE = 1000;
let bulkBody: any[] = [];
let bulkCount = 0;
let pendingIds: string[] = [];

async function flushBulk(reason = "flush") {
    if (bulkBody.length === 0) {
        return { ok: 0, fail: 0, succeededIds: [] as string[], failedIds: [] as string[] };
    }

    let res: any;
    try {
        res = await es.bulk({ body: bulkBody, refresh: "wait_for" });
    } catch (err: any) {
        const failedIds = pendingIds.slice();
        console.error(`BULK ${reason}: transport error`, err?.meta?.statusCode || err?.statusCode || err);
        bulkBody = [];
        bulkCount = 0;
        pendingIds = [];
        return { ok: 0, fail: failedIds.length, succeededIds: [], failedIds };
    }

    const items: any[] = Array.isArray(res?.items) ? res.items : [];
    const hasErrors = !!res?.errors;

    let ok = 0;
    const succeededIds: string[] = [];
    const failedIds: string[] = [];

    for (let i = 0; i < items.length; i++) {
        const action = items[i];
        const key = action && Object.keys(action)[0];
        const result = key ? action[key] : undefined;
        const hadError = !!result?.error;
        const evId = pendingIds[i];

        if (!hadError) {
            ok++;
            if (evId) succeededIds.push(evId);
        } else {
            if (evId) failedIds.push(evId);
            if (failedIds.length <= 3) {
                console.warn(
                    `BULK ${reason}: item error [${key}] id=${evId ?? result?._id} type=${result?.error?.type} reason=${result?.error?.reason}`
                );
            }
        }
    }

    console.log(`BULK ${reason}: ok=${ok} fail=${failedIds.length} errors=${hasErrors}`);

    bulkBody = [];
    bulkCount = 0;
    pendingIds = [];

    return { ok, fail: failedIds.length, succeededIds, failedIds };
}

function queueUpdate(indexName: string, id: string, doc: any) {
    bulkBody.push({
        update: { _index: indexName, _id: id, retry_on_conflict: 3, require_alias: true },
    });
    bulkBody.push({ doc });
    bulkCount++;
    pendingIds.push(id);
}

const EXCLUDED_CLIENT_ID = "0e57b625-ff35-11ef-8005-0ebcd8044491";

async function backfillScoreDetailsLocal() {
    await pg.connect();

    const processed = loadProgress();
    console.log(`Progress file: ${PROGRESS_FILE} (loaded ${processed.size} ids)`);
    console.log(`Failed file (JSONL): ${FAILED_FILE}`);

    let success = 0, failed = 0, skipped = 0;

    const cursor = pg.query(new Cursor(`
        SELECT 
            e.id AS evaluation_id,
            esd.category_id,
            esd.score,
            esd.justification,
            esd.phrases,
            c.slug AS category_slug,
            c.name AS category_name,
            COALESCE(cl.external_id, cc.external_id) AS external_client_id
        FROM evaluations e
        LEFT JOIN evaluation_score_details esd ON e.id = esd.evaluation_id
        LEFT JOIN score_detail_categories c ON esd.category_id = c.id
        LEFT JOIN clients cl ON e.client_id = cl.id
        LEFT JOIN contacts ct ON e.contact_id = ct.id
        LEFT JOIN clients cc ON ct.client_id = cc.id
    `));

    const BATCH_SIZE = 5000;

    function readNextBatch(): Promise<any[]> {
        return new Promise((resolve, reject) => {
            cursor.read(BATCH_SIZE, (err, rows) => {
                if (err) return reject(err);
                resolve(rows);
            });
        });
    }

    let rows: any[];
    do {
        rows = await readNextBatch();
        const grouped: Record<string, { clientId: string; details: ScoreDetailOut[] }> = {};

        for (const row of rows) {
            const evId = row.evaluation_id;
            if (processed.has(evId)) {
                skipped++;
                continue;
            }

            const externalClientId = (row.external_client_id || "").toLowerCase();
            if (!externalClientId || externalClientId === EXCLUDED_CLIENT_ID) {
                skipped++;
                continue;
            }

            if (!grouped[evId]) grouped[evId] = { clientId: externalClientId, details: [] };

            grouped[evId].details.push({
                categoryId: row.category_id,
                categorySlug: row.category_slug || undefined,
                categoryName: row.category_name || undefined,
                score: Math.round(Number(row.score)),
                justification: row.justification || undefined,
                phrases: Array.isArray(row.phrases) && row.phrases.length ? row.phrases : undefined,
            });
        }

        for (const [evId, { clientId, details }] of Object.entries(grouped)) {
            if (!details.length) continue;
            const indexName = `contact_evaluation___${clientId}`;
            queueUpdate(indexName, evId, { scoreDetails: details });

            if (bulkCount >= BULK_FLUSH_SIZE) {
                const { ok, fail, succeededIds, failedIds } = await flushBulk("periodic");
                success += ok;
                failed += fail;
                for (const id of succeededIds) processed.add(id);
                if (succeededIds.length) saveProgress(processed);
                if (failedIds.length) appendFailed(failedIds, "bulk-periodic");
            }
        }
    } while (rows.length > 0);

    const { ok, fail, succeededIds, failedIds } = await flushBulk("final");
    success += ok;
    failed += fail;
    for (const id of succeededIds) processed.add(id);
    if (succeededIds.length) saveProgress(processed);
    if (failedIds.length) appendFailed(failedIds, "bulk-final");

    console.log(`\nDone. Success=${success}, Failed=${failed}, Skipped=${skipped}`);
    console.log(`Progress saved: ${processed.size} ids`);
    console.log(`Failures appended to: ${FAILED_FILE}`);

    cursor.close(() => pg.end());
}

if (require.main === module) {
    backfillScoreDetailsLocal().catch((e) => {
        console.error(e);
        process.exit(1);
    });
}
