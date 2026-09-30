WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(device) AS device_upper,
        COALESCE(browser, 'N/A') AS browser_filled
    FROM sessions
    WHERE started_at > TO_TIMESTAMP('2023-06-13 18:00:00')
      AND country = 'UK'
)
-- Ranking and gap features
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    country,
    device_upper,
    LAST_VALUE(country) IGNORE NULLS OVER w_running AS last_known_country,
    ROW_NUMBER() OVER (PARTITION BY customer_id, device ORDER BY started_at DESC, id ASC) AS row_seq,
    FIRST_VALUE(country) IGNORE NULLS OVER w_part AS first_known_country,
    ROW_NUMBER() OVER (PARTITION BY customer_id, device ORDER BY started_at ASC, id ASC) AS row_seq_2,
    ROW_NUMBER() OVER (ORDER BY started_at ASC, id ASC) AS event_seq
FROM filtered
WINDOW w_running AS (PARTITION BY device ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
