WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec, id) AS latest_rank
    FROM web_events
    QUALIFY latest_rank = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        latest_rank,
        HOUR(event_time) AS event_time_hour,
        COALESCE(page, 'N/A') AS page_filled
    FROM latest
    WHERE event_time > DATE '2023-06-04'
      AND page IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        event_type,
        page,
        latest_rank,
        event_time_hour,
        page_filled,
        SUM(event_time_hour) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_time_hour,
        AVG(duration_sec) OVER (PARTITION BY page ORDER BY event_time_hour, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_3_prior
    FROM valid
),
scored_web_events AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY page ORDER BY duration_sec_avg_3_prior) AS duration_sec_avg_3_prior_rank,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM scored
    WHERE event_type <> 'purchase'
    WINDOW w_recent AS (ORDER BY duration_sec_avg_3_prior DESC, id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        event_type,
        page_filled,
        running_event_time_hour,
        duration_sec_avg_3_prior_rank,
        row_seq,
        MAX(row_seq) OVER (PARTITION BY event_type ORDER BY duration_sec_avg_3_prior_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_row_seq,
        COUNT(row_seq) OVER (PARTITION BY event_type ORDER BY duration_sec_avg_3_prior_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_row_seq,
        AVG(row_seq) OVER (PARTITION BY event_type ORDER BY duration_sec_avg_3_prior_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_row_seq
    FROM scored_web_events
)
SELECT
    id,
    session_id,
    event_type,
    page_filled,
    running_event_time_hour,
    running_avg_row_seq,
    CASE WHEN event_type RLIKE '^(add_to_cart|click)$' THEN 'EXTENDED' WHEN event_type RLIKE '^(page_view|purchase)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS event_type_zone
FROM with_windows;
