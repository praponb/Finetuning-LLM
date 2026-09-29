WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(page, 'N/A') AS page_filled,
        COALESCE(page, 'OTHER') AS page_filled_2
    FROM web_events
    WHERE event_time >= DATE '2023-04-01'
      AND event_time < '2024-03-19'
),
joined AS (
    -- Attach sessions attributes
    SELECT src.*, d.browser
    FROM base src
    LEFT JOIN sessions d
        ON src.session_id = d.id
    WHERE d.device IN ('desktop', 'tablet')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM joined
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        event_type,
        event_time,
        duration_sec,
        page_filled,
        SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_time,
        page_filled,
        burst_id,
        burst_id - duration_sec AS burst_id_minus_duration_sec
    FROM sessionized
    WHERE event_time >= DATE '2023-08-26'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(burst_id_minus_duration_sec) OVER w_part AS burst_id_minus_duration_sec_avg_2_prior,
        LAST_VALUE(burst_id_minus_duration_sec, TRUE) OVER (ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_burst_id_minus_duration_sec,
        LAG(burst_id, 2, 0) OVER w_ordered AS prev_burst_id
    FROM cleaned
    WINDOW w_part AS (ORDER BY burst_id, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_ordered AS (ORDER BY event_time, id)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        event_time,
        burst_id,
        burst_id_minus_duration_sec,
        last_known_burst_id_minus_duration_sec,
        prev_burst_id,
        MIN(prev_burst_id) OVER w_ordered AS running_min_prev_burst_id,
        COUNT(prev_burst_id) OVER w_ordered AS running_count_prev_burst_id,
        AVG(prev_burst_id) OVER w_ordered AS running_avg_prev_burst_id,
        MAX(prev_burst_id) OVER w_ordered AS running_max_prev_burst_id
    FROM metrics
    WINDOW w_ordered AS (ORDER BY prev_burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    event_time,
    burst_id,
    prev_burst_id,
    running_min_prev_burst_id,
    running_count_prev_burst_id,
    running_avg_prev_burst_id,
    running_max_prev_burst_id
FROM scored
