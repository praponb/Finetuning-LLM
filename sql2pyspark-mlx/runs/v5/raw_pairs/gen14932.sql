WITH base AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        LENGTH(page) AS page_length
    FROM web_events
    WHERE page IS NOT NULL
      AND page IS DISTINCT FROM '/home'
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY page_length DESC, id) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
with_windows AS (
    SELECT
        id,
        session_id,
        page,
        duration_sec,
        page_length,
        MIN(duration_sec) OVER (PARTITION BY session_id ORDER BY dedup_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_duration_sec,
        COUNT(duration_sec) OVER (PARTITION BY session_id ORDER BY dedup_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY dedup_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_duration_sec,
        LEAD(page_length, 2) OVER (PARTITION BY page ORDER BY duration_sec, id) AS next_page_length
    FROM ranked
),
joined AS (
    SELECT
        s.*,
        b.country,
        b.device
    FROM with_windows s
    INNER JOIN sessions b
        ON s.session_id = b.id
),
joined_web_events AS (
    SELECT
        src.id,
        src.session_id,
        src.page,
        src.page_length,
        src.last3_min_duration_sec,
        src.last3_sum_duration_sec,
        b.browser,
        b.device AS session_device,
        b.country AS session_country
    FROM joined src
    LEFT JOIN sessions b
        ON src.session_id = b.id
    WHERE b.country IN ('TH', 'DE', 'JP')
),
metrics AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY page_length DESC, id) AS page_length_bucket,
        CUME_DIST() OVER (PARTITION BY session_country ORDER BY last3_min_duration_sec DESC) AS last3_min_duration_sec_cume_dist,
        ROW_NUMBER() OVER w_recent AS seq_no,
        AVG(last3_sum_duration_sec) OVER (PARTITION BY session_device ORDER BY last3_sum_duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last3_sum_duration_sec_avg_6
    FROM joined_web_events
    WINDOW w_recent AS (PARTITION BY page ORDER BY last3_min_duration_sec, id)
)
SELECT
    id,
    session_id,
    page,
    last3_min_duration_sec,
    last3_sum_duration_sec,
    browser,
    session_device,
    session_country,
    last3_min_duration_sec_cume_dist
FROM metrics
