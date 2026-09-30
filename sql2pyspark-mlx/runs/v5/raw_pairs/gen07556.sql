WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(amount * 100, 2) AS amount_scaled,
        COALESCE(merchant, 'NONE') AS resolved_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    account_id,
    CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END AS is_new_burst_tier,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    PERCENTILE_APPROX(island_id, 0.95) AS island_id_p95,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    DENSE_RANK() OVER (PARTITION BY account_id ORDER BY PERCENTILE_APPROX(island_id, 0.95) DESC) AS island_id_p95_dense_rank
FROM islands
GROUP BY account_id, CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END
