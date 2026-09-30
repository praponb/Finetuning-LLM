WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        LOWER(city) AS city_lower,
        CASE WHEN segment IN ('Consumer', 'Corporate') THEN 'Core' ELSE 'Other' END AS segment_group
    FROM customers
    WHERE segment IS NOT NULL
      AND signup_date > TO_DATE('2023-02-06')
),
with_latest AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.segment,
        s.city_lower,
        x.session_country
    FROM base s
    JOIN LATERAL (
            SELECT c.country AS session_country
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.subscription_plan,
        x.subscription_start_date
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.plan AS subscription_plan, c.start_date AS subscription_start_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.start_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest_match
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY subscription_start_date, id) = 1
),
with_latest_customers AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.segment,
        s.city_lower,
        top1.subscription_plan_2,
        top1.monthly_fee
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.plan AS subscription_plan_2, c.monthly_fee
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.monthly_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    segment,
    CASE WHEN monthly_fee >= 249 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_tier,
    APPROX_PERCENTILE(monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS monthly_fee_quartiles,
    COUNT(DISTINCT subscription_plan_2) AS distinct_subscription_plan_2s,
    SUM(CASE WHEN segment = 'SMB' THEN monthly_fee ELSE 0 END) AS smb_monthly_fee,
    AVG(monthly_fee) FILTER (WHERE subscription_plan_2 = 'basic') AS basic_avg_monthly_fee,
    COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count
FROM with_latest_customers
GROUP BY segment, monthly_fee_tier
HAVING smb_monthly_fee < 1000000
ORDER BY basic_avg_monthly_fee DESC;
