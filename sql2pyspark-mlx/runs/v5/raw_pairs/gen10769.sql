WITH base AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), signup_date) days_since_signup_date,
        DATE_TRUNC('year', signup_date) signup_date_period
    FROM customers
    WHERE signup_date >= DATE '2023-09-19'
      AND city IS NOT NULL
),
kept AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
        )
),
with_last AS (
    SELECT
        s.id,
        s.email,
        s.country,
        s.city,
        s.segment,
        x.channel
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.channel
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date <= s.signup_date_period
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
filtered AS (
    SELECT
        id,
        email,
        country,
        channel,
        CASE WHEN city IN ('Berlin', 'New York') THEN 'Tier 1' ELSE 'Other' END city_group,
        INITCAP(TRIM(channel)) channel_title,
        COALESCE(channel, 'OTHER') resolved_channel
    FROM with_last
    WHERE country IN ('DE', 'JP', 'US')
),
valid AS (
    SELECT
        id,
        email,
        country,
        city_group,
        channel_title,
        LOWER(country) country_lower,
        COALESCE(channel, 'UNKNOWN') channel_clean
    FROM filtered
    WHERE country NOT IN ('DE', 'SG')
)
SELECT
    id,
    email,
    city_group,
    country_lower,
    channel_clean
FROM valid
