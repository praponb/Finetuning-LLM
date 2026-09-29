WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        TRIM(browser) AS clean_browser,
        COALESCE(country, 'UNKNOWN') AS country_clean
    FROM sessions
    WHERE started_at > TIMESTAMP '2024-02-22 00:00:00'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY started_at) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC NULLS LAST, id) AS rn
        FROM latest
    ) t
    WHERE rn = 1
),
latest_sessions AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY started_at DESC) AS row_num
    FROM ranked
    QUALIFY row_num = 1
)
SELECT
    s.id,
    s.customer_id,
    s.started_at,
    s.country,
    s.country_clean,
    s.rn,
    CASE WHEN country RLIKE '^(UK|US)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS country_zone,
    (SELECT AVG(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM latest_sessions s
ORDER BY rn DESC NULLS LAST
