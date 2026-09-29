WITH base AS (
    SELECT
        *,
        ROUND(duration_sec, 2) AS duration_sec_rounded,
        COALESCE(page, 'OTHER') AS page_clean
    FROM web_events
    WHERE page IS NOT NULL
),
scored AS (
    SELECT
        *,
        MAX(duration_sec_rounded) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_duration_sec_rounded,
        COUNT(duration_sec_rounded) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_duration_sec_rounded,
        MIN(duration_sec_rounded) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_duration_sec_rounded
    FROM base
),
with_gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM scored
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
filtered AS (
    SELECT
        *,
        SUBSTRING(event_type, 1, 3) AS event_type_prefix,
        COALESCE(page, 'N/A') AS resolved_page
    FROM islands
    WHERE event_time > TIMESTAMP '2023-05-24 17:00:00'
      AND duration_sec_rounded <= 77
),
with_prev AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_event_time
    FROM filtered
),
with_gaps_2 AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessionized AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        duration_sec_rounded,
        page_clean,
        running_max_duration_sec_rounded,
        running_count_duration_sec_rounded,
        running_min_duration_sec_rounded,
        new_group_flag,
        island_id,
        SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps_2
),
enriched AS (
    SELECT
        id,
        event_type,
        duration_sec_rounded,
        running_min_duration_sec_rounded,
        burst_id,
        SUM(island_id) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_island_id,
        MIN(island_id) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_island_id,
        AVG(island_id) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_island_id,
        MAX(island_id) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_island_id,
        RANK() OVER (ORDER BY running_max_duration_sec_rounded DESC NULLS FIRST) AS running_max_duration_sec_rounded_rank
    FROM sessionized
),
cleaned AS (
    SELECT
        id,
        event_type,
        duration_sec_rounded,
        running_min_duration_sec_rounded,
        burst_id,
        last7_avg_island_id,
        last7_max_island_id,
        COALESCE(last7_sum_island_id, 0) AS last7_sum_island_id_filled
    FROM enriched
    WHERE running_max_duration_sec_rounded_rank >= 7
)
SELECT
    id,
    event_type,
    running_min_duration_sec_rounded,
    burst_id,
    last7_avg_island_id
FROM cleaned;
