WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(merchant) AS merchant_lower
    FROM transactions
    WHERE merchant IS NOT NULL
      AND account_id IN (5, 19, 26, 30)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN amount >= 3500 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    COUNT_IF(merchant = 'Shell') AS shell_events,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END), 0), 2) AS amount_per_refund
FROM islands
GROUP BY 1
