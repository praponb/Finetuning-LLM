WITH with_windows AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY customer_id, browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country_2,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS event_seq
    FROM sessions
)
SELECT
    customer_id,
    browser,
    PERCENTILE_APPROX(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min
FROM with_windows
GROUP BY customer_id, browser
