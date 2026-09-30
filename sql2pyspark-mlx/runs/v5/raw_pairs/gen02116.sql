WITH valid AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        CONCAT(event_type, ' - ', page) event_type_page
    FROM web_events
    WHERE page IS NOT NULL
),
with_windows AS (
    SELECT
        *,
        MIN(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_min_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_avg_duration_sec,
        COUNT(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_count_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) last4_max_duration_sec,
        DENSE_RANK() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS FIRST) duration_sec_dense_rank
    FROM valid
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY page ORDER BY last4_count_duration_sec) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
),
enriched AS (
    SELECT
        src.id,
        src.session_id,
        src.event_time,
        src.duration_sec,
        src.last4_avg_duration_sec,
        b.country,
        b.browser
    FROM latest src
    JOIN sessions b
        ON src.session_id = b.id
),
scored AS (
    SELECT
        *,
        NTILE(3) OVER w_recent duration_sec_bucket,
        AVG(duration_sec) OVER w_grp duration_sec_avg_2,
        AVG(last4_avg_duration_sec) OVER (PARTITION BY country, session_id ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last4_avg_duration_sec_avg_2,
        RANK() OVER (PARTITION BY session_id ORDER BY last4_avg_duration_sec DESC NULLS LAST) last4_avg_duration_sec_rank
    FROM enriched
    WHERE country NOT IN ('TH', 'SG', 'JP')
    WINDOW w_recent AS (PARTITION BY session_id, country ORDER BY duration_sec DESC, id), w_grp AS (PARTITION BY country ORDER BY last4_avg_duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    session_id,
    duration_sec,
    last4_avg_duration_sec,
    country,
    browser,
    duration_sec_bucket,
    duration_sec_avg_2,
    last4_avg_duration_sec_avg_2
FROM scored;
