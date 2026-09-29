WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        ROW_NUMBER() OVER w_running seq_no,
        LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY customer_id, country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_browser,
        LAST_VALUE(browser, TRUE) OVER (PARTITION BY browser, country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_browser_2,
        ROW_NUMBER() OVER (ORDER BY started_at DESC, id) event_seq
    FROM sessions
    WINDOW w_running AS (ORDER BY started_at, id)
)
SELECT
    id,
    seq_no,
    last_known_browser,
    last_known_browser_2
FROM scored
ORDER BY id DESC
