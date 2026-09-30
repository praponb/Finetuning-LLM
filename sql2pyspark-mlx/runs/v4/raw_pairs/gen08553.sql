WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN amount >= 3600 THEN 'High' WHEN amount >= 1900 THEN 'Medium' ELSE 'Low' END AS amount_level,
        CONCAT_WS('/', txn_type, merchant) AS txn_type_merchant_key
    FROM transactions
    WHERE merchant IS NOT NULL
      AND amount > 2800
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS new_group_flag_tier,
    SUM(amount) AS total_amount,
    AVG(new_group_flag) AS avg_new_group_flag
FROM sessionized
GROUP BY CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING AVG(new_group_flag) < 0
ORDER BY new_group_flag_tier DESC
