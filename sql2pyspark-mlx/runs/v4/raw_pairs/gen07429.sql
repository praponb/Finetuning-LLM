WITH cleaned AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        LENGTH(browser) AS browser_length
    FROM sessions
    WHERE started_at >= DATE '2023-12-10'
      AND browser IS NOT NULL
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    country,
    browser_length,
    SUM(browser_length) OVER w_ordered AS running_browser_length,
    ROW_NUMBER() OVER (ORDER BY started_at, id) AS event_seq,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY browser_length NULLS FIRST) AS browser_length_dense_rank
FROM cleaned
WINDOW w_ordered AS (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
