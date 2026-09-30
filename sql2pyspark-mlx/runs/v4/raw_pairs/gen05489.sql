WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(status, 1, 3) AS status_prefix
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-12-05 19:00:00'
      AND order_id IS NOT NULL
      AND amount BETWEEN 1180 AND 1430
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        amount,
        status,
        status_prefix,
        new_group_flag,
        burst_id,
        DATE_TRUNC('year', paid_at) AS paid_at_period
    FROM islands
    WHERE amount > 1530
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        method,
        MAX(paid_at_period) AS ended_at,
        SUM(CASE WHEN status = 'FAILED' THEN amount ELSE 0 END) AS failed_amount
    FROM cleaned
    GROUP BY method
)
SELECT method, CASE WHEN method RLIKE '^(promptpay|paypal)$' THEN 'OTHER_ZONE' WHEN method RLIKE '^(card|bank_transfer)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS method_zone
FROM stats
ORDER BY method_zone NULLS FIRST, method;
