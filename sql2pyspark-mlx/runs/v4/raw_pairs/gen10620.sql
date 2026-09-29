WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(monthly_fee, 2) OVER (PARTITION BY status ORDER BY start_date, id) AS prev_monthly_fee,
        FIRST_VALUE(monthly_fee, TRUE) OVER (ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_monthly_fee,
        AVG(monthly_fee) OVER (PARTITION BY plan) AS plan_avg_monthly_fee
    FROM subscriptions
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY end_date DESC NULLS LAST, id) = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM ranked src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.start_date >= sub.start_date
       AND src.start_date <= DATE_ADD(sub.end_date, 10)
)
-- Grouped metrics
SELECT
    subscription_id,
    PERCENTILE_APPROX(prev_monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS prev_monthly_fee_quartiles,
    PERCENTILE_APPROX(subscription_id, ARRAY(0.25, 0.5, 0.75)) AS subscription_id_quartiles
FROM attributed
GROUP BY 1;
