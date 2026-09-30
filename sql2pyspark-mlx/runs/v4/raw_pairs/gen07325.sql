WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(country, TRUE) OVER w_running AS last_known_country,
        LAST_VALUE(browser, TRUE) OVER w_grp AS last_known_browser,
        LAST_VALUE(country) IGNORE NULLS OVER w_part AS last_known_country_2,
        FIRST_VALUE(country) IGNORE NULLS OVER w_running AS first_known_country
    FROM sessions
    WINDOW w_running AS (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY browser, customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    device,
    started_at,
    country,
    last_known_country,
    last_known_browser,
    last_known_country_2,
    first_known_country,
    CASE WHEN device RLIKE '^(tablet|mobile)$' THEN 'GROUP_A' WHEN device RLIKE '^(desktop)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS device_zone
FROM scored
