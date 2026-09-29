WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time,
        COALESCE(page, 'NONE') AS resolved_page
    FROM web_events
    WHERE page IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY days_since_event_time, id) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        event_time,
        days_since_event_time,
        AVG(duration_sec) OVER w_grp AS duration_sec_avg_30d,
        LAST_VALUE(days_since_event_time) IGNORE NULLS OVER (PARTITION BY event_type, session_id ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_event_time,
        CUME_DIST() OVER (PARTITION BY event_type ORDER BY duration_sec DESC) AS duration_sec_cume_dist,
        RANK() OVER (ORDER BY duration_sec NULLS LAST) AS duration_sec_rank,
        PERCENT_RANK() OVER w_ordered AS duration_sec_percent_rank
    FROM ranked
    WINDOW w_grp AS (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY session_id, page ORDER BY duration_sec DESC NULLS LAST)
),
scored_web_events AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(duration_sec_percent_rank, 1) OVER (ORDER BY event_time, id) AS prev_duration_sec_percent_rank,
        FIRST_VALUE(duration_sec_avg_30d, TRUE) OVER w_part AS first_known_duration_sec_avg_30d
    FROM scored
    WINDOW w_part AS (ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        days_since_event_time,
        duration_sec_rank,
        duration_sec_percent_rank,
        prev_duration_sec_percent_rank,
        first_known_duration_sec_avg_30d,
        CASE WHEN duration_sec_avg_30d >= 367 THEN 'High' WHEN duration_sec_avg_30d >= 82 THEN 'Medium' ELSE 'Low' END AS duration_sec_avg_30d_band
    FROM scored_web_events
    WHERE days_since_event_time BETWEEN 33 AND 58
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(duration_sec_percent_rank) OVER w_grp AS cumulative_duration_sec_percent_rank,
        AVG(days_since_event_time) OVER (ORDER BY first_known_duration_sec_avg_30d, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS days_since_event_time_avg_3,
        SUM(duration_sec_rank) OVER (ORDER BY prev_duration_sec_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_rank,
        DENSE_RANK() OVER (ORDER BY duration_sec_rank DESC) AS duration_sec_rank_dense_rank
    FROM base
    WINDOW w_grp AS (ORDER BY first_known_duration_sec_avg_30d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    days_since_event_time,
    duration_sec_rank,
    first_known_duration_sec_avg_30d,
    duration_sec_avg_30d_band,
    cumulative_duration_sec_percent_rank,
    cumulative_duration_sec_rank,
    duration_sec_rank_dense_rank
FROM metrics
