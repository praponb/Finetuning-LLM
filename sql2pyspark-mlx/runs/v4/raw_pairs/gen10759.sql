WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(country) AS country_length
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-06-30 05:00:00'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM filtered a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.started_at >= sub.start_date
       AND a.started_at < sub.end_date + INTERVAL 14 DAYS
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(subscription_id, 0) AS subscription_id_filled,
        COALESCE(plan, 'OTHER') AS plan_filled
    FROM matched
    WHERE customer_id IN (7, 8, 29, 40)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    customer_id,
    country,
    SIZE(COLLECT_SET(device)) AS num_devices,
    COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count,
    SUM(subscription_id_filled) AS total_subscription_id_filled,
    SUM(country_length) FILTER (WHERE plan = 'premium') AS premium_total_country_length,
    COUNT(CASE WHEN device = 'tablet' THEN 1 END) AS tablet_count,
    COUNT(*) AS event_count
FROM cleaned
GROUP BY DATE_TRUNC('MONTH', started_at), customer_id, country
HAVING total_subscription_id_filled < 0
ORDER BY country
