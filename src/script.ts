import { Client as PgClient } from "pg";
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
        console.warn(
            `Could not read progress file; starting fresh (${PROGRESS_FILE})`,
            e,
        );
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

const BULK_FLUSH_SIZE = 3;
let bulkBody: any[] = [];
let bulkCount = 0;
let pendingIds: string[] = [];

async function flushBulk(reason = "flush") {
    if (bulkBody.length === 0) {
        return { ok: 0, fail: 0, succeededIds: [] as string[], failedIds: [] as string[] };
    }

    let res: any;
    try {
        res = await es.bulk({
            body: bulkBody,
            refresh: "wait_for",
        });
        console.log(res)
    } catch (err: any) {
        const failedIds = pendingIds.slice();
        console.error(`BULK ${reason}: transport error`, err?.meta?.statusCode || err?.statusCode || err);
        bulkBody = [];
        bulkCount = 0;
        pendingIds = [];
        return { ok: 0, fail: failedIds.length, succeededIds: [], failedIds };
    }

    const items: any[] = Array.isArray(res?.items) ? res.items : [];
    const took = res?.took;
    const hasErrors = !!res?.errors;

    let ok = 0;
    const succeededIds: string[] = [];
    const failedIds: string[] = [];

    if (items.length !== pendingIds.length) {
        console.warn(
            `BULK ${reason}: items length ${items.length} != pendingIds length ${pendingIds.length} (check bulk body pairing)`
        );
    }
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

    const fail = items.length - ok;
    console.log(`BULK ${reason}: items=${items.length} ok=${ok} fail=${fail} errors=${hasErrors} took=${took}ms`);

    bulkBody = [];
    bulkCount = 0;
    pendingIds = [];

    return { ok, fail, succeededIds, failedIds };
}

function queueUpdate(indexName: string, id: string, doc: any) {
    bulkBody.push({
        update: { _index: indexName, _id: id, retry_on_conflict: 3, require_alias: true, },
    });
    bulkBody.push({ doc });
    bulkCount++;
    pendingIds.push(id);
}
const EXCLUDED_CLIENT_ID = "0e57b625-ff35-11ef-8005-0ebcd8044491";
const MAX_REQUESTS = 3

async function backfillScoreDetailsLocal() {
    await pg.connect();

    const processed = loadProgress();
    console.log(`Progress file: ${PROGRESS_FILE} (loaded ${processed.size} ids)`);
    console.log(`Failed file (JSONL): ${FAILED_FILE}`);
    if (MAX_REQUESTS) console.log(`MAX_REQUESTS=${MAX_REQUESTS}`);

    try {
        const { rows: rawIds } = await pg.query<{ evaluation_id: string }>(
            `SELECT DISTINCT evaluation_id FROM evaluation_score_details`
        );

        const evaluationIds = rawIds.map((r) => r.evaluation_id);
        console.log(`Found ${evaluationIds.length} evaluation_ids.`);

        let success = 0;
        let failed = 0;
        let skipped = 0;
        let processedCount = 0;

        for (const evaluationId of evaluationIds) {
            if (MAX_REQUESTS && processedCount >= MAX_REQUESTS) {
                console.log(`Reached MAX_REQUESTS=${MAX_REQUESTS}, stopping early.`);
                break;
            }

            if (processed.has(evaluationId)) {
                skipped++;
                continue;
            }

            const { rows: details } = await pg.query<any>(
                `SELECT esd.category_id, esd.score, esd.justification, esd.phrases,
                c.slug as category_slug, c.name as category_name
         FROM evaluation_score_details esd
         LEFT JOIN score_detail_categories c ON esd.category_id = c.id
         WHERE esd.evaluation_id = $1`,
                [evaluationId]
            );
            if (!details.length) {
                skipped++;
                continue;
            }

            const scoreDetails: ScoreDetailOut[] = details
                .map((d) => ({
                    categoryId: d.category_id,
                    categorySlug: d.category_slug || undefined,
                    categoryName: d.category_name || undefined,
                    score: Math.round(Number(d.score)),
                    justification: d.justification || undefined,
                    phrases: Array.isArray(d.phrases) && d.phrases.length ? d.phrases : undefined,
                }))
                .filter(
                    (d) =>
                        d.categorySlug ||
                        d.categoryName ||
                        d.justification ||
                        (Array.isArray(d.phrases) && d.phrases.length > 0)
                );
            if (!scoreDetails.length) {
                skipped++;
                continue;
            }

            const { rows: evRows } = await pg.query<any>(
                `SELECT e.id,
                cl.external_id as client_external_id,
                cc.external_id as contact_client_external_id
         FROM evaluations e
         LEFT JOIN clients cl  ON e.client_id  = cl.id
         LEFT JOIN contacts ct ON e.contact_id = ct.id
         LEFT JOIN clients cc  ON ct.client_id = cc.id
         WHERE e.id = $1`,
                [evaluationId]
            );
            const ev = evRows[0];
            const externalClientId = (ev?.client_external_id || ev?.contact_client_external_id || "").toLowerCase();

            if (!externalClientId || externalClientId === EXCLUDED_CLIENT_ID) {
                skipped++;
                continue;
            }

            const indexName = `contact_evaluation__${externalClientId}`;
            queueUpdate(indexName, evaluationId, { scoreDetails });
            processedCount++;

            if (bulkCount >= BULK_FLUSH_SIZE) {
                const { ok, fail, succeededIds, failedIds } = await flushBulk("periodic");
                success += ok;
                failed += fail;

                let added = 0;
                for (const id of succeededIds) {
                    if (!processed.has(id)) {
                        processed.add(id);
                        added++;
                    }
                }
                if (added > 0) saveProgress(processed);

                if (failedIds.length) appendFailed(failedIds, "bulk-periodic");
            }
        }

        const { ok, fail, succeededIds, failedIds } = await flushBulk("final");
        success += ok;
        failed += fail;

        let added = 0;
        for (const id of succeededIds) {
            if (!processed.has(id)) {
                processed.add(id);
                added++;
            }
        }
        if (added > 0) saveProgress(processed);

        if (failedIds.length) appendFailed(failedIds, "bulk-final");

        const summary = {
            success,
            failed,
            skipped,
            total: evaluationIds.length,
            saved: processed.size,
            failedFile: FAILED_FILE,
        };
        console.log(
            `\nDone. Success: ${success}, Failed: ${failed}, Skipped: ${skipped}, Total: ${evaluationIds.length}`
        );
        console.log(`Progress saved: ${processed.size} ids`);
        console.log(`Failures appended to: ${FAILED_FILE}`);
        return summary;
    } finally {
        await pg.end();
    }
}

if (require.main === module) {
    backfillScoreDetailsLocal().catch((e) => {
        console.error(e);
        process.exit(1);
    });
}
