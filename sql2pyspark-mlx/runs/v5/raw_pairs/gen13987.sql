WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(duration_sec, 2) AS duration_sec_rounded,
        COALESCE(page, 'N/A') AS page_clean
    FROM web_events
    WHERE event_time > TO_DATE('2023-03-14')
      AND event_time < '2025-09-01'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id, page ORDER BY duration_sec_rounded DESC NULLS LAST, id) AS rank_in_group
        FROM islands
    ) ranked
    WHERE rank_in_group = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT event_type, page
    FROM ranked
    EXCEPT
    SELECT event_type, page
    FROM web_events
    WHERE session_id IN (12, 29)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.event_type = k.event_type AND s.page = k.page
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY event_type, session_id ORDER BY session_no DESC) AS session_no_dense_rank,
        COUNT(session_no) OVER (PARTITION BY session_id) AS session_id_count_session_no
    FROM kept_rows
)
-- Join a dimension and aggregate
SELECT
    d.device,
    e.page,
    COUNT(*) AS num_rows,
    MAX(e.session_no) AS max_session_no,
    MIN(e.duration_sec_rounded) AS min_duration_sec_rounded,
    GROUPING_ID() AS level_id
FROM metrics e
INNER JOIN sessions d ON e.session_id = d.id
GROUP BY GROUPING SETS ((d.device, e.page), (d.device), ())
ORDER BY max_session_no DESC NULLS LAST
