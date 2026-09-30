WITH joined AS (
    -- Attach sessions attributes
    SELECT a.*, b.country
    FROM web_events a
    INNER JOIN sessions b
        ON a.session_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN duration_sec >= 512 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev
FROM joined
GROUP BY ROLLUP (CASE WHEN duration_sec >= 512 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', event_time))
HAVING avg_duration_sec < 0
ORDER BY activity_month NULLS FIRST;
