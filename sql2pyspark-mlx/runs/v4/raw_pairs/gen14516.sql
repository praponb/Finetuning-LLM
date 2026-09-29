WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY txn_time, id) AS dedup_rank
        FROM transactions
    ) t
    WHERE dedup_rank = 1
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        p.status,
        p.account_type
    FROM deduped s
    LEFT JOIN accounts p
        ON s.account_id = p.id
),
joined AS (
    -- Attach accounts attributes
    SELECT a.*, b.opened_date
    FROM enriched a
    LEFT JOIN accounts b
        ON a.account_id = b.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount >= 4000 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    account_id,
    PERCENTILE_APPROX(amount, 0.9) AS amount_p90,
    MAX(txn_time) AS last_txn_time
FROM joined
GROUP BY CASE WHEN amount >= 4000 THEN 'HIGH' ELSE 'LOW' END, account_id
HAVING PERCENTILE_APPROX(amount, 0.9) >= 0
ORDER BY account_id;
