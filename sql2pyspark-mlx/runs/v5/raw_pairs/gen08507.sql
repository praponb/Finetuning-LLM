WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_FORMAT(start_date, 'MMM yyyy') AS start_date_label
    FROM subscriptions
    WHERE end_date >= TO_DATE('2023-11-14')
      AND plan = 'enterprise'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC NULLS FIRST, id) = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM ranked s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.end_date >= sub.start_date
       AND s.end_date <= DATE_ADD(sub.end_date, 10)
),
attributed_subscriptions AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan_2,
        sub.monthly_fee AS subscription_monthly_fee_2
    FROM attributed s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.end_date BETWEEN sub.start_date AND sub.end_date
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.plan,
        s.end_date,
        s.monthly_fee,
        s.status,
        s.start_date_label,
        s.subscription_id,
        s.subscription_plan,
        s.subscription_monthly_fee,
        s.subscription_id_2,
        d.city,
        d.signup_date,
        d.email
    FROM attributed_subscriptions s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN monthly_fee >= 195 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_tier,
        SIZE(COLLECT_SET(plan)) AS num_plans,
        COUNT(*) FILTER (WHERE status = 'ACTIVE') AS active_rows,
        STDDEV_SAMP(monthly_fee) AS monthly_fee_stddev,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids,
        SIZE(COLLECT_SET(city)) AS num_cities
    FROM with_dims
    GROUP BY CASE WHEN monthly_fee >= 195 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    monthly_fee_tier,
    num_plans,
    active_rows,
    monthly_fee_stddev,
    num_cities
FROM totals
ORDER BY monthly_fee_stddev DESC NULLS LAST
