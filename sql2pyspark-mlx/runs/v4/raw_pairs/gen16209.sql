WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country, browser ORDER BY started_at DESC, id) AS row_seq,
        LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        ROW_NUMBER() OVER w_ordered AS row_seq_2,
        FIRST_VALUE(browser, TRUE) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY browser, customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
    FROM sessions
    WINDOW w_ordered AS (PARTITION BY device ORDER BY started_at DESC, id)
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.country,
    s.last_known_country,
    s.first_known_country,
    CASE WHEN device RLIKE '^(desktop|tablet)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS device_zone,
    (SELECT AVG(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM enriched s
ORDER BY device_zone NULLS FIRST
