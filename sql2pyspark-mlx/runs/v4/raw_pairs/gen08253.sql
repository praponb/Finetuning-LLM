SELECT
    CASE WHEN duration_sec >= 510 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    APPROX_PERCENTILE(duration_sec, 0.5) AS duration_sec_p50,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    COUNT_IF(event_type = 'page_view') AS page_view_events,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN page = '/product' THEN 1 END), 0), 2) AS duration_sec_per_product,
    PERCENTILE_APPROX(duration_sec, 0.5) AS duration_sec_p50_2
FROM web_events
GROUP BY 1
HAVING COUNT_IF(event_type = 'page_view') > 1
ORDER BY duration_sec_tier DESC NULLS FIRST;
