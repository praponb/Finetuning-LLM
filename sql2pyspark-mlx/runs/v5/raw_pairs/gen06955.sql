WITH scored AS (
    -- Running and moving aggregates
    SELECT *, LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country
    FROM sessions
    WHERE started_at >= '2025-04-20'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        browser,
        started_at
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC NULLS FIRST, id) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(browser) AS clean_browser
    FROM ranked
    WHERE browser IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) = 1
)
SELECT
    s.id,
    s.customer_id,
    s.started_at,
    s.clean_browser,
    (SELECT MIN(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM deduped s
ORDER BY started_at DESC, min_subscription_monthly_fee
