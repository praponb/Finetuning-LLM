WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        INITCAP(TRIM(page)) page_title
    FROM web_events
    WHERE page IS NOT NULL
      AND page IS DISTINCT FROM '/product'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec, id) event_seq,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC, id) event_seq_2
    FROM latest
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_time,
        duration_sec,
        page_title,
        row_num,
        event_seq,
        MIN(event_seq) OVER w_ordered last7_min_event_seq,
        SUM(event_seq) OVER w_ordered last7_sum_event_seq,
        COUNT(event_seq) OVER w_ordered last7_count_event_seq
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(last7_sum_event_seq) last7_sum_event_seq_whole
    FROM with_windows
    WHERE event_time >= TO_TIMESTAMP('2023-03-25 20:00:00')
),
base_web_events AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_time,
        duration_sec,
        row_num,
        last7_min_event_seq,
        last7_sum_event_seq,
        last7_sum_event_seq_whole,
        ROUND(last7_sum_event_seq_whole / event_seq, 2) last7_sum_event_seq_whole_per_event_seq
    FROM cleaned
    WHERE TO_DATE(event_time) < '2024-08-01'
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base_web_events
    EXCEPT
    SELECT id
    FROM web_events
    WHERE event_time < '2024-03-08'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base_web_events s
    INNER JOIN matched_keys k ON s.id = k.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    DATE_TRUNC('YEAR', event_time) AS year_start,
    CASE WHEN duration_sec >= 367 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    PERCENTILE_APPROX(last7_sum_event_seq_whole_per_event_seq, ARRAY(0.25, 0.5, 0.75)) AS last7_sum_event_seq_whole_per_event_seq_quartiles,
    ROUND(AVG(last7_sum_event_seq), 2) AS avg_last7_sum_event_seq,
    MAX(last7_sum_event_seq) AS max_last7_sum_event_seq,
    ROUND(AVG(last7_sum_event_seq), 2) AS avg_last7_sum_event_seq_2
FROM kept_rows
GROUP BY 1, 2, 3;
