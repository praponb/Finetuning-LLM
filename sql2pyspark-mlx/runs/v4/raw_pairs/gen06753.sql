WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC NULLS FIRST, id) AS rn
    FROM transactions
    QUALIFY rn = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount >= 3700 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    merchant,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    COUNT(CASE WHEN merchant = '7-Eleven' THEN 1 END) AS 7_eleven_count,
    COUNT(DISTINCT account_id) AS distinct_account_ids,
    COUNT(*) FILTER (WHERE merchant = 'Shell') AS shell_rows,
    APPROX_PERCENTILE(amount, 0.5) AS amount_p50,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types
FROM deduped
GROUP BY CASE WHEN amount >= 3700 THEN 'LARGE' ELSE 'SMALL' END, merchant
HAVING amount_p50 < 0
ORDER BY distinct_account_ids NULLS FIRST;
