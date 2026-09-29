WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM web_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY page ORDER BY duration_sec NULLS FIRST) AS duration_sec_percent_rank,
        LEAD(island_id, 1) OVER (PARTITION BY page ORDER BY duration_sec, id) AS next_island_id,
        LAG(island_id, 1, 0) OVER (PARTITION BY page ORDER BY duration_sec, id) AS prev_island_id
    FROM sessionized
    WHERE HOUR(event_time) BETWEEN 10 AND 14
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(duration_sec, 1) OVER (PARTITION BY page ORDER BY duration_sec, id) AS next_duration_sec,
        DENSE_RANK() OVER (PARTITION BY page ORDER BY prev_island_id DESC NULLS LAST) AS prev_island_id_dense_rank,
        DENSE_RANK() OVER (ORDER BY prev_island_id DESC NULLS FIRST) AS prev_island_id_dense_rank_2
    FROM metrics
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN prev_island_id >= 8 THEN 'High' WHEN prev_island_id >= 4 THEN 'Medium' ELSE 'Low' END AS prev_island_id_band,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM scored
    WHERE event_time >= DATE '2023-04-19'
      AND next_duration_sec <= 185
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        event_time,
        duration_sec,
        island_id,
        next_island_id,
        next_duration_sec,
        page_filled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY prev_island_id_dense_rank DESC NULLS LAST, id) AS dedup_rank
        FROM cleaned
    ) t
    WHERE dedup_rank = 1
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('YEAR', event_time) AS year_start,
        (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
        (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min
    FROM ranked
    GROUP BY year_start
)
SELECT
    year_start,
    span_minutes,
    duration_min
FROM grouped;
