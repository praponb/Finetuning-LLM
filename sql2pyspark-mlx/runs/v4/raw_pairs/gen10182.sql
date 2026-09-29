WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        CASE WHEN country = 'JP' THEN 1 ELSE 0 END AS is_jp
    FROM sessions
    WHERE customer_id IN (10, 12, 34, 40)
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM base a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.started_at >= sub.start_date
       AND a.started_at < sub.end_date + INTERVAL 14 DAYS
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM matched s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time <= s.started_at
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, browser ORDER BY subscription_id DESC, id ASC, subscription_id ASC) AS rn
        FROM not_flagged
    ) x
    WHERE rn = 1
)
SELECT
    id,
    customer_id,
    started_at,
    country,
    subscription_id,
    plan,
    monthly_fee,
    rn
FROM latest
