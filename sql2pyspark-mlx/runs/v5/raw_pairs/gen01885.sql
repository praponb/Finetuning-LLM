WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) AS dedup_rank
    FROM sessions
    QUALIFY dedup_rank = 1
)
SELECT
    DATE_TRUNC('WEEK', started_at) AS week_start,
    customer_id,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    COUNT_IF(country = 'UK') AS uk_events,
    COUNT(*) AS row_count,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes,
    GROUPING_ID() AS grouping_id
FROM latest
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', started_at), customer_id), (DATE_TRUNC('WEEK', started_at)))
HAVING uk_events > 1
