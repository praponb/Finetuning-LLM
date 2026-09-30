WITH base AS (
    SELECT *, DATE_TRUNC('week', event_time) AS event_time_period
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-06-12 14:00:00'
),
scored AS (
    SELECT
        *,
        LEAD(duration_sec) OVER w_running AS next_duration_sec,
        LAG(duration_sec) OVER w_running AS prev_duration_sec,
        ROW_NUMBER() OVER w_running AS seq_no
    FROM base
    WINDOW w_running AS (PARTITION BY event_type ORDER BY duration_sec, id)
),
gaps AS (
    SELECT *, CASE WHEN event_time_period - LAG(event_time_period) OVER (PARTITION BY event_type ORDER BY event_time_period, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM scored
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
metrics AS (
    SELECT
        id,
        event_type,
        event_time,
        event_time_period,
        is_new_burst,
        island_id,
        NTILE(3) OVER (PARTITION BY session_id, event_type ORDER BY is_new_burst DESC, id) AS is_new_burst_bucket,
        LAST_VALUE(seq_no, TRUE) OVER (PARTITION BY session_id ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_seq_no,
        NTILE(4) OVER (PARTITION BY event_type ORDER BY next_duration_sec DESC, id) AS next_duration_sec_bucket,
        RANK() OVER w_ordered AS seq_no_rank,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY island_id DESC) AS island_id_dense_rank
    FROM islands
    WINDOW w_ordered AS (PARTITION BY page, event_type ORDER BY seq_no DESC NULLS FIRST)
),
matched_keys AS (
    SELECT event_type, id
    FROM metrics
    EXCEPT ALL
    SELECT event_type, id
    FROM web_events
    WHERE duration_sec > 305
),
reconciled AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.event_type = k.event_type AND s.id = k.id
)
SELECT
    island_id,
    CASE WHEN last_known_seq_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_seq_no_tier,
    AVG(next_duration_sec_bucket) AS avg_next_duration_sec_bucket,
    COUNT_IF(event_type = 'search') AS search_events,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    APPROX_PERCENTILE(next_duration_sec_bucket, ARRAY(0.25, 0.5, 0.75)) AS next_duration_sec_bucket_quartiles,
    MAX(event_time) AS last_event_time
FROM reconciled
GROUP BY GROUPING SETS ((island_id, CASE WHEN last_known_seq_no >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END), (island_id))
HAVING num_event_types > 1
ORDER BY avg_next_duration_sec_bucket;
