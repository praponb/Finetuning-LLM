WITH valid AS (
    SELECT *, COALESCE(duration_sec, 0) duration_sec_filled
    FROM web_events
    WHERE page IS NOT NULL
),
deduped AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) = 1
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec_filled DESC NULLS LAST, id) AS rn
    FROM deduped
    QUALIFY rn = 1
),
scored AS (
    SELECT
        *,
        RANK() OVER w_recent duration_sec_filled_rank,
        SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_duration_sec,
        AVG(duration_sec) OVER w_part page_avg_duration_sec
    FROM latest
    WINDOW w_recent AS (PARTITION BY page, event_type ORDER BY duration_sec_filled DESC NULLS FIRST), w_part AS (PARTITION BY page)
),
enriched AS (
    SELECT
        src.*,
        b.browser,
        b.country
    FROM scored src
    LEFT JOIN sessions b
        ON src.session_id = b.id
),
enriched_web_events AS (
    SELECT
        a.*,
        p.country AS session_country,
        p.browser AS session_browser,
        p.device
    FROM enriched a
    INNER JOIN sessions p
        ON a.session_id = p.id
)
SELECT
    id,
    page,
    event_time,
    duration_sec,
    duration_sec_filled,
    rn,
    duration_sec_filled_rank,
    cumulative_duration_sec,
    page_avg_duration_sec
FROM enriched_web_events
ORDER BY rn NULLS FIRST;
