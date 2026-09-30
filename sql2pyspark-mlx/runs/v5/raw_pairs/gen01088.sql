WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_type,
        event_time,
        is_new_burst,
        burst_id,
        RANK() OVER (PARTITION BY event_type ORDER BY burst_id) AS burst_id_rank,
        LAG(is_new_burst, 2, 0) OVER (PARTITION BY event_type, page ORDER BY duration_sec, id) AS prev_is_new_burst,
        COUNT(duration_sec) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_count_7d,
        LEAD(duration_sec, 1) OVER (PARTITION BY page, session_id ORDER BY burst_id, id) AS next_duration_sec
    FROM islands
    WHERE burst_id >= 4
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM enriched
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        event_type,
        event_time,
        burst_id,
        burst_id_rank,
        next_duration_sec,
        SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM flagged
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, event_type
    FROM sessionized
    EXCEPT
    SELECT id, event_type
    FROM web_events
    WHERE duration_sec < 120
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.event_type = k.event_type
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(burst_id) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS burst_id_avg_7d,
        PERCENT_RANK() OVER (ORDER BY burst_id_rank DESC NULLS LAST) AS burst_id_rank_percent_rank,
        COUNT(burst_id_2) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS burst_id_2_count_7d
    FROM kept_rows
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN burst_id_2 >= 9 THEN 'HIGH' ELSE 'LOW' END AS burst_id_2_tier,
        APPROX_PERCENTILE(burst_id, ARRAY(0.25, 0.5, 0.75)) AS burst_id_quartiles,
        COUNT_IF(event_type = 'add_to_cart') AS add_to_cart_events,
        PERCENTILE_APPROX(burst_id_rank_percent_rank, 0.95) AS burst_id_rank_percent_rank_p95,
        APPROX_PERCENTILE(burst_id_2_count_7d, 0.5) AS burst_id_2_count_7d_p50
    FROM with_windows
    GROUP BY CASE WHEN burst_id_2 >= 9 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    burst_id_2_tier,
    burst_id_quartiles,
    add_to_cart_events,
    burst_id_2_count_7d_p50
FROM summary
ORDER BY burst_id_2_count_7d_p50 DESC NULLS LAST
