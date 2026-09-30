WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        FLOOR(monthly_fee) AS monthly_fee_whole,
        INITCAP(TRIM(status)) AS status_title
    FROM subscriptions
    WHERE plan = 'standard'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        status,
        status_title,
        AVG(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_monthly_fee,
        MAX(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_monthly_fee,
        SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_monthly_fee
    FROM filtered
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(last3_sum_monthly_fee) AS last3_sum_monthly_fee_whole
    FROM with_windows
    WHERE customer_id IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM cleaned
    EXCEPT
    SELECT customer_id
    FROM subscriptions
    WHERE status IN ('EXPIRED', 'ACTIVE', 'CANCELLED')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.customer_id = k.customer_id
)
-- Join a dimension and aggregate
SELECT
    p.country,
    p.city,
    COUNT(*) AS rows_total,
    ROUND(SUM(e.last3_avg_monthly_fee), 2) AS total_last3_avg_monthly_fee,
    MIN(e.last3_sum_monthly_fee_whole) AS min_last3_sum_monthly_fee_whole
FROM kept_rows e
LEFT JOIN customers p ON e.customer_id = p.id
GROUP BY p.country, p.city
ORDER BY country NULLS LAST
