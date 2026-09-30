WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS(',', status, plan) AS status_plan_key,
        CASE WHEN status IN ('EXPIRED', 'ACTIVE') THEN 'Core' ELSE 'Other' END AS status_group
    FROM subscriptions
    WHERE start_date >= DATE '2023-11-13'
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.segment,
        c.email,
        c.signup_date
    FROM base s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(monthly_fee) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY end_date, id) AS row_seq
    FROM enriched
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, status
    FROM scored
    INTERSECT ALL
    SELECT id, status
    FROM subscriptions
    WHERE end_date BETWEEN '2024-03-17' AND '2024-06-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN matched_keys k ON s.id = k.id AND s.status = k.status
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('WEEK', signup_date) AS week_start,
        COUNT_IF(segment = 'Corporate') AS corporate_events,
        SUM(cumulative_monthly_fee) FILTER (WHERE plan = 'standard') AS standard_total_cumulative_monthly_fee,
        STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids,
        COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows
    FROM verified
    GROUP BY week_start
    HAVING basic_rows > 1
)
SELECT
    week_start,
    monthly_fee_stddev,
    basic_rows
FROM stats
ORDER BY basic_rows NULLS LAST;
