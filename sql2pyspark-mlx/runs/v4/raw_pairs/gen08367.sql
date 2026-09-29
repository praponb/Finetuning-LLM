SELECT
    CASE WHEN duration_sec >= 532 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    page,
    DATE_TRUNC('WEEK', event_time) AS week_start,
    COUNT_IF(event_type = 'search') AS search_events,
    SIZE(COLLECT_SET(page)) AS num_pages,
    MAX(event_time) AS ended_at,
    COUNT(*) FILTER (WHERE page = '/checkout') AS checkout_rows,
    PERCENTILE_APPROX(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    SUM(CASE WHEN page = '/cart' THEN duration_sec ELSE 0 END) AS cart_duration_sec
FROM web_events
GROUP BY ROLLUP (CASE WHEN duration_sec >= 532 THEN 'LARGE' ELSE 'SMALL' END, page, DATE_TRUNC('WEEK', event_time));
