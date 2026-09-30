WITH remaining_keys AS (
    SELECT page
    FROM web_events
    EXCEPT ALL
    SELECT page
    FROM web_events
    WHERE event_type = 'page_view'
),
kept_rows AS (
    SELECT s.*
    FROM web_events s
    JOIN remaining_keys k ON s.page = k.page
)
SELECT
    page,
    PERCENTILE_APPROX(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    BIT_OR(duration_sec) AS duration_sec_bit_or,
    SUM(duration_sec) FILTER (WHERE page = '/product') AS product_total_duration_sec
FROM kept_rows
GROUP BY page
ORDER BY product_total_duration_sec NULLS FIRST
