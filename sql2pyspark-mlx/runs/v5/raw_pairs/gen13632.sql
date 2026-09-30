WITH filtered AS (
    SELECT
        id,
        session_id,
        page,
        duration_sec,
        DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time
    FROM web_events
    WHERE page IS DISTINCT FROM '/search'
),
cleaned AS (
    SELECT
        id,
        session_id,
        page,
        duration_sec,
        LEAST(days_since_event_time, duration_sec) AS least_days_since_event_time_duration_sec
    FROM filtered
    WHERE days_since_event_time <> 45
),
valid AS (
    SELECT
        id,
        page,
        duration_sec,
        CASE WHEN least_days_since_event_time_duration_sec >= 70 THEN 'High' WHEN least_days_since_event_time_duration_sec >= 11 THEN 'Medium' ELSE 'Low' END AS least_days_since_event_time_duration_sec_level
    FROM cleaned
    WHERE page IS DISTINCT FROM '/search'
      AND least_days_since_event_time_duration_sec BETWEEN 39 AND 62
),
scored AS (
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        ROW_NUMBER() OVER (ORDER BY duration_sec DESC, id) AS event_seq,
        NTILE(4) OVER (PARTITION BY page ORDER BY duration_sec, id) AS duration_sec_bucket,
        CUME_DIST() OVER (ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_cume_dist
    FROM valid
),
deduped AS (
    SELECT
        id,
        page,
        least_days_since_event_time_duration_sec_level,
        cumulative_duration_sec,
        duration_sec_bucket,
        duration_sec_cume_dist
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec_cume_dist NULLS FIRST, id) AS latest_rank
        FROM scored
    ) t
    WHERE latest_rank = 1
)
SELECT
    CASE WHEN cumulative_duration_sec >= 1883 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_duration_sec_tier,
    CASE WHEN duration_sec_cume_dist >= 0.68 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_cume_dist_tier,
    page,
    ROUND(AVG(duration_sec_bucket), 2) AS avg_duration_sec_bucket,
    SORT_ARRAY(COLLECT_SET(page)) AS page_list,
    COUNT(CASE WHEN page = '/home' THEN 1 END) AS home_count,
    COUNT(DISTINCT page) AS distinct_pages,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(CASE WHEN page = '/cart' THEN 1 END) AS cart_count,
    GROUPING(CASE WHEN cumulative_duration_sec >= 1883 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_id
FROM deduped
GROUP BY ROLLUP (CASE WHEN cumulative_duration_sec >= 1883 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN duration_sec_cume_dist >= 0.68 THEN 'HIGH' ELSE 'LOW' END, page)
HAVING ROUND(AVG(duration_sec_bucket), 2) < 1000000
ORDER BY cart_count DESC NULLS FIRST;
