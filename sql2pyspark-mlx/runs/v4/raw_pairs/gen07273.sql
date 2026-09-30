WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        CASE WHEN device = 'tablet' THEN 1 ELSE 0 END AS is_tablet,
        COALESCE(country, 'NONE') AS country_clean
    FROM sessions
    WHERE started_at > TO_TIMESTAMP('2023-07-18 04:00:00')
)
-- Non-equi join on a validity window
SELECT
    e.*,
    sub.id AS subscription_id,
    sub.plan,
    sub.monthly_fee
FROM cleaned e
INNER JOIN subscriptions sub
    ON e.customer_id = sub.customer_id
   AND e.started_at >= sub.start_date
   AND e.started_at <= DATE_ADD(sub.end_date, 3);
