WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_recent AS duration_sec_rank,
        ROW_NUMBER() OVER w_running AS seq_no
    FROM web_events
    WINDOW w_recent AS (ORDER BY duration_sec DESC NULLS FIRST), w_running AS (PARTITION BY page ORDER BY duration_sec, id)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_windows
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        duration_sec_rank,
        seq_no,
        island_id,
        CAST(duration_sec AS STRING) AS duration_sec_num,
        COALESCE(page, 'OTHER') AS page_clean
    FROM sessions_web_events
    WHERE event_time > DATE '2023-11-14'
      AND page IS NOT NULL
      AND is_new_burst <> 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(page, TRUE) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_page,
        SUM(duration_sec) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER w_running AS last_known_duration_sec,
        NTILE(4) OVER w_grp AS duration_sec_bucket
    FROM base
    WINDOW w_running AS (PARTITION BY event_type, session_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY page ORDER BY duration_sec DESC, id)
),
base_web_events AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        island_id + cumulative_duration_sec AS island_id_plus_cumulative_duration_sec,
        COALESCE(page, 'OTHER') AS page_clean_2
    FROM scored
    WHERE cumulative_duration_sec >= 1809
),
remaining_keys AS (
    -- Set difference on keys
    SELECT session_id
    FROM base_web_events
    EXCEPT
    SELECT session_id
    FROM web_events
    WHERE session_id IN (20, 26, 38, 40)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base_web_events s
    JOIN remaining_keys k ON s.session_id = k.session_id
),
scored_web_events AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        duration_sec_rank,
        seq_no,
        island_id,
        duration_sec_num,
        page_clean,
        first_known_page,
        cumulative_duration_sec,
        last_known_duration_sec,
        duration_sec_bucket,
        island_id_plus_cumulative_duration_sec,
        page_clean_2,
        STDDEV_SAMP(seq_no) OVER (PARTITION BY session_id) AS session_id_stddev_samp_seq_no,
        CUME_DIST() OVER (ORDER BY duration_sec_bucket DESC NULLS FIRST) AS duration_sec_bucket_cume_dist,
        DENSE_RANK() OVER (PARTITION BY event_type, session_id ORDER BY cumulative_duration_sec) AS cumulative_duration_sec_dense_rank,
        PERCENT_RANK() OVER (ORDER BY duration_sec_rank DESC NULLS LAST) AS duration_sec_rank_percent_rank
    FROM reconciled
)
SELECT
    id,
    session_id,
    event_type,
    duration_sec,
    duration_sec_rank,
    island_id,
    duration_sec_num,
    page_clean,
    first_known_page,
    CASE WHEN event_type RLIKE '^(purchase|page_view)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS event_type_zone
FROM scored_web_events
ORDER BY island_id DESC
