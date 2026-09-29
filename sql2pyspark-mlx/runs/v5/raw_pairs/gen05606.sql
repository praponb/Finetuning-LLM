WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        CASE WHEN txn_type IN ('REFUND', 'CREDIT') THEN 'Priority' ELSE 'Other' END AS txn_type_group
    FROM transactions
    WHERE merchant IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        txn_type_group,
        SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    CASE WHEN island_id >= 6 THEN 'HIGH' ELSE 'LOW' END AS island_id_tier,
    SUM(island_id) AS total_island_id,
    COUNT(*) AS cnt,
    GROUPING(DATE_TRUNC('YEAR', txn_time)) AS grouping_id
FROM sessions_transactions
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', txn_time), CASE WHEN island_id >= 6 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('YEAR', txn_time)), ())
ORDER BY island_id_tier NULLS LAST
