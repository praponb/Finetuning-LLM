WITH not_flagged AS (
    SELECT s.*
    FROM sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < s.started_at
        )
),
remaining_keys AS (
    SELECT customer_id
    FROM not_flagged
    EXCEPT ALL
    SELECT customer_id
    FROM sessions
    WHERE customer_id IN (10, 14, 35)
),
kept_rows AS (
    SELECT s.*
    FROM not_flagged s
    JOIN remaining_keys k ON s.customer_id = k.customer_id
),
cleaned AS (
    SELECT
        *,
        UPPER(device) AS device_upper,
        DATE_TRUNC('year', started_at) AS started_at_period
    FROM kept_rows
    WHERE browser IS NOT NULL
),
kept AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.page NOT IN ('/checkout', '/search', '/home')
        )
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.started_at,
    s.country,
    s.started_at_period,
    (SELECT AVG(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM kept s;
