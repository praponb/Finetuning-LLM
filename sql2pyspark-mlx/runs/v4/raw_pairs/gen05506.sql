SELECT
    *,
    LAST_VALUE(country, TRUE) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
    ROW_NUMBER() OVER (ORDER BY started_at DESC, id ASC) AS seq_no,
    FIRST_VALUE(browser, TRUE) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser
FROM sessions
