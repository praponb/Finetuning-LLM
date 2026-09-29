WITH filtered AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        DATE_FORMAT(event_time, 'yyyy-MM-dd') AS event_time_label,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-11-15 22:00:00'
      AND page IS NOT NULL
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec_filled DESC NULLS LAST, id) AS latest_rank
        FROM filtered
    ) x
    WHERE latest_rank = 1
),
enriched AS (
    SELECT
        src.*,
        s.browser,
        s.country,
        s.device
    FROM deduped src
    LEFT JOIN sessions s
        ON src.session_id = s.id
),
base AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        event_time_label,
        duration_sec_filled,
        UPPER(country) AS country_upper
    FROM enriched
    WHERE session_id IN (4, 5, 13, 23)
),
joined AS (
    SELECT s.*, p.device
    FROM base s
    LEFT JOIN sessions p
        ON s.session_id = p.id
),
enriched_web_events AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        event_time_label,
        duration_sec_filled,
        country_upper,
        device,
        CUME_DIST() OVER (PARTITION BY page ORDER BY duration_sec_filled NULLS LAST) AS duration_sec_filled_cume_dist,
        SUM(duration_sec_filled) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_filled,
        LEAD(duration_sec, 2) OVER (PARTITION BY page ORDER BY duration_sec, id) AS next_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec_filled DESC, id) AS event_seq,
        AVG(duration_sec_filled) OVER (PARTITION BY session_id) AS session_id_avg_duration_sec_filled
    FROM joined
),
with_windows AS (
    SELECT
        *,
        NTILE(10) OVER (ORDER BY event_seq, id) AS event_seq_bucket,
        LAG(duration_sec_filled_cume_dist, 2) OVER (PARTITION BY device ORDER BY session_id_avg_duration_sec_filled, id) AS prev_duration_sec_filled_cume_dist,
        LEAD(duration_sec_filled, 2) OVER (PARTITION BY device ORDER BY duration_sec_filled, id) AS next_duration_sec_filled,
        SUM(duration_sec_filled) OVER (PARTITION BY session_id ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_filled_2
    FROM enriched_web_events
),
stats AS (
    SELECT
        device,
        ROUND(SUM(prev_duration_sec_filled_cume_dist) / NULLIF(COUNT(CASE WHEN page = '/home' THEN 1 END), 0), 2) AS prev_duration_sec_filled_cume_dist_per_home,
        SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
    FROM with_windows
    GROUP BY device
)
SELECT
    device,
    prev_duration_sec_filled_cume_dist_per_home,
    event_type_list
FROM stats
ORDER BY device NULLS LAST;
