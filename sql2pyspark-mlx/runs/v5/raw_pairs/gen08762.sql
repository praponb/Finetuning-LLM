WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        SUBSTRING(method, 1, 3) AS method_prefix,
        CASE WHEN method IN ('cash', 'promptpay') THEN 'Priority' ELSE 'Other' END AS method_group
    FROM payments
    WHERE paid_at > TO_TIMESTAMP('2023-07-26 07:00:00')
      AND method IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    COUNT_IF(status = 'FAILED') AS failed_events,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min,
    COUNT(CASE WHEN method = 'card' THEN 1 END) AS card_count,
    SUM(CASE WHEN method = 'bank_transfer' THEN is_new_burst ELSE 0 END) AS bank_transfer_is_new_burst
FROM islands
GROUP BY 1
