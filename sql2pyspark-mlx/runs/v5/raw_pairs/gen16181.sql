WITH base AS (
    SELECT
        *,
        CASE WHEN page IN ('/search', '/cart') THEN 'Priority' ELSE 'Other' END AS page_group,
        COALESCE(page, 'OTHER') AS page_filled
    FROM web_events
    WHERE event_time >= '2024-10-01'
),
gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
),
scored AS (
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        page_group,
        is_new_session,
        MIN(is_new_session) OVER w_grp AS last3_min_is_new_session,
        COUNT(is_new_session) OVER w_grp AS last3_count_is_new_session,
        MAX(is_new_session) OVER w_grp AS last3_max_is_new_session,
        SUM(is_new_session) OVER w_grp AS last3_sum_is_new_session
    FROM sessionized
    WINDOW w_grp AS (PARTITION BY event_type ORDER BY is_new_session, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        id,
        session_id,
        event_type,
        page_group,
        last3_count_is_new_session,
        LAG(last3_count_is_new_session) OVER w_part AS prev_last3_count_is_new_session,
        LEAD(last3_sum_is_new_session) OVER w_part AS next_last3_sum_is_new_session,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM scored
    WHERE last3_max_is_new_session < 0.73
    WINDOW w_part AS (PARTITION BY event_type ORDER BY last3_count_is_new_session, id)
),
metrics AS (
    SELECT
        id,
        session_id,
        event_type,
        page_group,
        last3_count_is_new_session,
        prev_last3_count_is_new_session,
        next_last3_sum_is_new_session,
        seq_no,
        MAX(seq_no) OVER w_hist AS running_max_seq_no,
        MIN(seq_no) OVER w_hist AS running_min_seq_no,
        SUM(seq_no) OVER w_hist AS running_sum_seq_no
    FROM enriched
    WINDOW w_hist AS (PARTITION BY session_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_web_events AS (
    SELECT
        *,
        SUM(prev_last3_count_is_new_session) OVER w_part AS running_sum_prev_last3_count_is_new_session,
        MIN(prev_last3_count_is_new_session) OVER w_part AS running_min_prev_last3_count_is_new_session,
        COUNT(prev_last3_count_is_new_session) OVER w_part AS running_count_prev_last3_count_is_new_session
    FROM metrics
    WINDOW w_part AS (PARTITION BY session_id ORDER BY running_min_seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    event_type,
    page_group,
    prev_last3_count_is_new_session,
    next_last3_sum_is_new_session,
    seq_no,
    running_max_seq_no,
    running_min_seq_no,
    running_sum_seq_no,
    CASE WHEN event_type RLIKE '^(click|search)$' THEN 'CORE' ELSE 'PRIORITY' END AS event_type_zone
FROM scored_web_events
ORDER BY running_min_seq_no, running_sum_seq_no DESC;
