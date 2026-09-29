WITH base AS (
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        COALESCE(amount, 0) amount_filled,
        COALESCE(merchant, 'UNKNOWN') resolved_merchant
    FROM transactions
    WHERE txn_time > TIMESTAMP '2023-07-25 16:00:00'
      AND merchant IS NOT NULL
),
with_prev AS (
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM base
),
gaps AS (
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessions_transactions AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS session_start_flag_tier,
    account_id,
    SUM(island_id) FILTER (WHERE merchant = 'Amazon') AS amazon_total_island_id,
    COUNT(DISTINCT account_id) AS distinct_account_ids,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    COUNT(DISTINCT id) AS distinct_ids,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    PERCENTILE_APPROX(island_id, 0.95) AS island_id_p95,
    GROUPING(CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END) AS gid
FROM sessions_transactions
GROUP BY CUBE (CASE WHEN session_start_flag >= 1 THEN 'HIGH' ELSE 'LOW' END, account_id)
ORDER BY session_start_flag_tier NULLS LAST;
