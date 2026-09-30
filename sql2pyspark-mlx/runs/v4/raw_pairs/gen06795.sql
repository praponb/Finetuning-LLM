WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        INITCAP(TRIM(method)) AS method_title
    FROM payments
    WHERE paid_at >= DATE '2024-01-16'
      AND method IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst_2
    FROM sessionized
),
sessionized_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst_2) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN session_no >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_no_tier,
    COUNT(*) FILTER (WHERE method = 'cash') AS cash_rows,
    COUNT(*) FILTER (WHERE method = 'paypal') AS paypal_rows,
    MAX(is_new_burst_2) FILTER (WHERE status = 'SUCCESS') AS success_max_is_new_burst_2
FROM sessionized_payments
GROUP BY 1
HAVING paypal_rows >= 1
ORDER BY cash_rows DESC;
