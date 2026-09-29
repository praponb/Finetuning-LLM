WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(page) AS page_length,
        COALESCE(page, 'OTHER') AS page_filled
    FROM web_events
    WHERE event_type = 'purchase'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        page,
        event_time,
        page_length,
        page_filled,
        duration_sec - page_length AS duration_sec_minus_page_length,
        page_length - duration_sec AS page_length_minus_duration_sec,
        COALESCE(page, 'NONE') AS page_clean
    FROM cleaned
    WHERE page IN ('/checkout', '/cart', '/product')
      AND event_time >= TIMESTAMP '2023-07-21 05:00:00'
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.page_length,
        e.duration_sec_minus_page_length,
        e.page_clean,
        s.browser
    FROM valid e
    LEFT JOIN sessions s
        ON e.session_id = s.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM enriched
    INTERSECT
    SELECT id
    FROM web_events
    WHERE duration_sec BETWEEN 181 AND 374
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN duration_sec_minus_page_length >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_minus_page_length_tier,
    PERCENTILE_APPROX(duration_sec_minus_page_length, 0.9) AS duration_sec_minus_page_length_p90,
    SUM(duration_sec_minus_page_length) FILTER (WHERE browser = 'Firefox') AS firefox_total_duration_sec_minus_page_length,
    SIZE(COLLECT_SET(browser)) AS num_browsers
FROM reconciled
GROUP BY CASE WHEN duration_sec_minus_page_length >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY firefox_total_duration_sec_minus_page_length
