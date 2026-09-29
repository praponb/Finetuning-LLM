WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        CASE WHEN event_type IN ('page_view', 'add_to_cart') THEN 'Tier 1' ELSE 'Other' END AS event_type_group
    FROM web_events
    WHERE event_time > DATE '2023-10-13'
      AND page IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(duration_sec, 0) AS duration_sec_filled_2
    FROM sessionized
    WHERE page IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY page, session_id ORDER BY burst_id DESC) AS burst_id_cume_dist,
        NTILE(5) OVER (ORDER BY duration_sec_filled DESC, id) AS duration_sec_filled_bucket
    FROM cleaned
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time NULLS FIRST) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
)
-- Window metrics over several different partitions
SELECT
    id,
    event_type,
    page,
    duration_sec_filled,
    event_type_group,
    new_group_flag,
    duration_sec_filled_2,
    burst_id_cume_dist,
    dedup_rank,
    ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec_filled_bucket, id) AS event_seq,
    MAX(duration_sec) OVER (PARTITION BY page) AS page_max_duration_sec
FROM ranked
WHERE HOUR(event_time) BETWEEN 6 AND 14;
