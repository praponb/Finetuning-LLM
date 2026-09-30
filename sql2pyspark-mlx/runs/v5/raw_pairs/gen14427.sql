SELECT
    *,
    LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY device, browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
    FIRST_VALUE(country, TRUE) OVER (PARTITION BY customer_id, country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
FROM sessions
