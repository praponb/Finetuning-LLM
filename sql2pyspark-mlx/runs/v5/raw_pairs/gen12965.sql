WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        UPPER(merchant) AS merchant_upper,
        COALESCE(merchant, 'OTHER') AS resolved_merchant
    FROM transactions
    WHERE merchant IS NOT DISTINCT FROM 'Lazada'
      AND amount > 2700
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        merchant_upper,
        resolved_merchant,
        SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN island_id >= 7 THEN 'LARGE' ELSE 'SMALL' END AS island_id_tier,
    MAX(txn_time) AS ended_at,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END), 0), 2) AS amount_per_amazon,
    COUNT_IF(merchant = 'Shell') AS shell_events,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN merchant = 'Lazada' THEN 1 END), 0), 2) AS amount_per_lazada
FROM sessions_transactions
GROUP BY CASE WHEN island_id >= 7 THEN 'LARGE' ELSE 'SMALL' END;
