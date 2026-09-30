WITH with_windows AS (
    SELECT
        *,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        LAST_VALUE(browser, TRUE) OVER (PARTITION BY customer_id, browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser
    FROM sessions
)
SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    COUNT(*) FILTER (WHERE browser = 'Chrome') AS chrome_rows,
    COUNT_IF(country = 'JP') AS jp_events,
    MIN(started_at) AS first_started_at
FROM with_windows
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', started_at))
ORDER BY quarter_start DESC NULLS FIRST
