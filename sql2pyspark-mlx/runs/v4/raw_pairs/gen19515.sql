WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS row_seq,
        LAST_VALUE(browser, TRUE) OVER (PARTITION BY device, country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        FIRST_VALUE(country) IGNORE NULLS OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        FIRST_VALUE(country, TRUE) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country_2,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) AS event_seq
    FROM sessions
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        row_seq,
        last_known_browser,
        first_known_country,
        first_known_country_2,
        event_seq,
        INITCAP(TRIM(device)) AS device_title,
        HOUR(started_at) AS started_at_hour
    FROM enriched
    WHERE customer_id IS NOT NULL
      AND event_seq > 5
),
enriched_sessions AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER w_ordered AS event_seq_dense_rank,
        NTILE(4) OVER (PARTITION BY country ORDER BY event_seq, id) AS event_seq_bucket
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY country ORDER BY event_seq DESC NULLS LAST)
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched_sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    PERCENTILE_APPROX(event_seq_bucket, ARRAY(0.25, 0.5, 0.75)) AS event_seq_bucket_quartiles,
    MIN(event_seq) AS min_event_seq
FROM sessionized
GROUP BY DATE_TRUNC('MONTH', started_at)
ORDER BY min_event_seq NULLS FIRST;
