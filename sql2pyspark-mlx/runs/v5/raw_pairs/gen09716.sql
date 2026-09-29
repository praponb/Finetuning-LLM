WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(monthly_fee, 0) AS monthly_fee_filled,
        CEIL(monthly_fee) AS monthly_fee_whole
    FROM subscriptions
    WHERE start_date >= DATE '2023-03-30'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee DESC NULLS FIRST, id) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
ranked_subscriptions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT id, start_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date, id) AS dedup_rank
        FROM ranked
    ) x
    WHERE dedup_rank = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label
    FROM ranked_subscriptions
    WHERE start_date BETWEEN '2024-04-01' AND '2024-11-01'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, QUARTER(start_date) AS start_date_quarter
    FROM base
    WHERE start_date >= DATE '2023-03-08'
),
base_subscriptions AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), start_date) AS days_since_start_date
    FROM cleaned
    WHERE start_date BETWEEN '2023-07-18' AND '2023-11-01'
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', start_date) AS week_start,
    ROUND(AVG(days_since_start_date), 2) AS avg_days_since_start_date,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(start_date_quarter, ARRAY(0.25, 0.5, 0.75)) AS start_date_quarter_quartiles,
    APPROX_PERCENTILE(days_since_start_date, 0.9) AS days_since_start_date_p90,
    ROUND(AVG(days_since_start_date), 2) AS avg_days_since_start_date_2
FROM base_subscriptions
GROUP BY CUBE (DATE_TRUNC('WEEK', start_date))
HAVING avg_days_since_start_date_2 >= 0
ORDER BY week_start DESC
