WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(event_type, ' ', page) AS event_type_page
    FROM web_events
    WHERE event_type = 'click'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        page,
        duration_sec,
        LAG(duration_sec) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS row_seq
    FROM cleaned
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN prev_duration_sec >= 436 THEN 'High' WHEN prev_duration_sec >= 149 THEN 'Medium' ELSE 'Low' END AS prev_duration_sec_tier,
        COALESCE(page, 'DIRECT') AS page_clean
    FROM metrics
    WHERE row_seq > 3
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM valid
    QUALIFY RANK() OVER (PARTITION BY page ORDER BY prev_duration_sec DESC) = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT session_id
    FROM deduped
    INTERSECT
    SELECT session_id
    FROM web_events
    WHERE page IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.session_id = k.session_id
),
metrics_web_events AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        event_seq,
        row_seq,
        DENSE_RANK() OVER w_running AS duration_sec_dense_rank,
        SUM(duration_sec) OVER (PARTITION BY session_id) AS session_id_sum_duration_sec,
        SUM(event_seq) OVER (PARTITION BY page) AS page_sum_event_seq
    FROM kept_rows
    WINDOW w_running AS (ORDER BY duration_sec NULLS FIRST)
)
-- Multi-level aggregation
SELECT
    session_id,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    ROUND(AVG(duration_sec_dense_rank), 2) AS avg_duration_sec_dense_rank,
    COUNT(DISTINCT session_id) AS distinct_session_ids,
    ROUND(AVG(duration_sec_dense_rank), 2) AS avg_duration_sec_dense_rank_2,
    COUNT(*) AS row_count,
    ROUND(AVG(page_sum_event_seq), 2) AS avg_page_sum_event_seq
FROM metrics_web_events
GROUP BY session_id
ORDER BY avg_row_seq DESC;
