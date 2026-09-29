WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, YEAR(end_date) AS end_date_year
    FROM subscriptions
    WHERE end_date > DATE '2023-09-17'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        status,
        end_date_year,
        COALESCE(end_date_year, 0) AS end_date_year_filled
    FROM filtered
    WHERE end_date_year IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date_year,
        SUM(end_date_year) OVER (ORDER BY start_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_end_date_year,
        PERCENT_RANK() OVER (ORDER BY end_date_year_filled ASC) AS end_date_year_filled_percent_rank
    FROM valid
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee
    FROM enriched a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.start_date >= sub.start_date
       AND a.start_date < sub.end_date + INTERVAL '30' DAY
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date_year,
        cumulative_end_date_year,
        end_date_year_filled_percent_rank,
        subscription_id,
        subscription_plan,
        monthly_fee,
        subscription_id / NULLIF(end_date_year, 0) AS subscription_id_per_end_date_year
    FROM with_window
    WHERE subscription_plan = 'basic'
)
-- Multi-level aggregation
SELECT
    CASE WHEN monthly_fee >= 244 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_tier,
    COUNT(CASE WHEN subscription_plan = 'premium' THEN 1 END) AS premium_count,
    ROUND(AVG(cumulative_end_date_year), 2) AS avg_cumulative_end_date_year,
    COUNT_IF(plan = 'enterprise') AS enterprise_events
FROM base
GROUP BY monthly_fee_tier
ORDER BY enterprise_events DESC;
