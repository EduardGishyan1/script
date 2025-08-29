import { Client as PgClient } from "pg";
import "dotenv/config";
import { Client as EsClient } from "@elastic/elasticsearch";

const pg = new PgClient({
    host: process.env.DB_HOST,
    port: Number(process.env.DB_PORT || 5432),
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
});

const es = new EsClient({
    node: process.env.ELASTIC_URL,
    auth: {
        username: process.env.ELASTIC_USER!,
        password: process.env.ELASTIC_PASS!,
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

async function backfillScoreDetailsLocal() {
    await pg.connect();

    try {
        const { rows: rawIds } = await pg.query<{
            evaluation_id: string;
        }>(
            `SELECT DISTINCT evaluation_id 
             FROM evaluation_score_details`,
        );

        const evaluationIds = rawIds.map((r) => r.evaluation_id);
        console.log(`Found ${evaluationIds.length} evaluation_ids.`);

        let success = 0;
        let failed = 0;
        let skipped = 0;

        for (const evaluationId of evaluationIds) {
            const { rows: details } = await pg.query<any>(
                `SELECT esd.category_id, esd.score, esd.justification, esd.phrases,
                c.slug as category_slug, c.name as category_name
                FROM evaluation_score_details esd
                LEFT JOIN score_detail_categories c
                ON esd.category_id = c.id
                WHERE esd.evaluation_id = $1`,
                [evaluationId],
            );

            if (!details.length) {
                skipped++;
                console.warn(`Skipped ${evaluationId}: no scoreDetails`);
                continue;
            }

            const scoreDetails: ScoreDetailOut[] = details
                .map((d) => ({
                    categoryId: d.category_id,
                    categorySlug: d.category_slug || undefined,
                    categoryName: d.category_name || undefined,
                    score: Math.round(Number(d.score)),
                    justification: d.justification || undefined,
                    phrases: d.phrases?.length ? d.phrases : undefined,
                }))
                .filter(
                    (d) =>
                        d.categorySlug ||
                        d.categoryName ||
                        d.justification ||
                        (Array.isArray(d.phrases) && d.phrases.length > 0),
                );

            if (!scoreDetails.length) {
                skipped++;
                console.warn(`Skipped ${evaluationId}: all scoreDetails filtered out`);
                continue;
            }

            const { rows: evRows } = await pg.query<any>(
                `SELECT e.id, cl.external_id as client_external_id, cc.external_id as contact_client_external_id
         FROM evaluations e
         LEFT JOIN clients cl ON e.client_id = cl.id
         LEFT JOIN contacts ct ON e.contact_id = ct.id
         LEFT JOIN clients cc ON ct.client_id = cc.id
         WHERE e.id = $1`,
                [evaluationId],
            );

            const ev = evRows[0];
            const externalClientId =
                ev?.client_external_id || ev?.contact_client_external_id || null;

            if (!externalClientId) {
                skipped++;
                console.warn(`Skipped ${evaluationId}: missing externalClientId`);
                continue;
            }

            try {
                console.log(scoreDetails)
                const indexName = `contact_evaluation__${externalClientId}`;
                console.log(indexName)

                await es.update({
                    index: indexName,
                    id: evaluationId,
                    doc: { scoreDetails },
                });

                console.log(`Updated evaluation_id=${evaluationId}`);
                success++;
            } catch (err) {
                failed++;
                console.error(`Failed evaluation_id=${evaluationId}`, err);
            }
        }

        const summary = { success, failed, skipped, total: evaluationIds.length };
        console.log(`\nDone. Success: ${success}, Failed: ${failed}, Skipped: ${skipped}`);
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
