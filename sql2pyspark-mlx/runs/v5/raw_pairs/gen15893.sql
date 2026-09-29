SELECT
    id,
    started_at,
    browser,
    LAST_VALUE(browser) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_browser
FROM sessions
WHERE customer_id IS NOT NULL
