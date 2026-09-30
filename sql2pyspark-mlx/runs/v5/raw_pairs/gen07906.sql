WITH with_windows AS (
    SELECT
        id,
        browser,
        country,
        LAST_VALUE(browser, TRUE) OVER (PARTITION BY country, browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        ROW_NUMBER() OVER (PARTITION BY device, browser ORDER BY started_at DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY started_at, id) AS event_seq_2,
        LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser_2
    FROM sessions
)
SELECT
    id,
    last_known_browser,
    last_known_browser_2
FROM with_windows
