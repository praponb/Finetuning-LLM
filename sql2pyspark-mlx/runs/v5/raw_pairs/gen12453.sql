WITH filtered AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        LOWER(page) page_lower,
        HOUR(event_time) event_time_hour,
        COALESCE(page, 'DIRECT') page_filled
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-07-04 12:00:00'
      AND page IS NOT NULL
),
valid AS (
    SELECT *, CAST(event_time AS DATE) event_time_day
    FROM filtered
    WHERE page IS NOT NULL
      AND page IS DISTINCT FROM '/product'
),
metrics AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        page_lower,
        event_time_hour,
        page_filled,
        event_time_day,
        CUME_DIST() OVER (PARTITION BY session_id ORDER BY duration_sec NULLS LAST) duration_sec_cume_dist,
        AVG(duration_sec) OVER (PARTITION BY session_id, event_type ORDER BY event_time_hour, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) duration_sec_avg_6_prior
    FROM valid
),
with_dims AS (
    SELECT
        e.*,
        d.browser,
        d.device
    FROM metrics e
    INNER JOIN sessions d
        ON e.session_id = d.id
),
joined AS (
    SELECT
        s.*,
        p.browser AS session_browser,
        p.device AS session_device,
        p.country
    FROM with_dims s
    LEFT JOIN sessions p
        ON s.session_id = p.id
    WHERE p.customer_id IN (18, 19, 33)
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_device ORDER BY event_time DESC NULLS LAST, id) AS dedup_rank
        FROM joined
    ) x
    WHERE dedup_rank = 1
),
gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM deduped
),
sessions_web_events AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    id,
    page,
    event_time,
    event_time_hour,
    page_filled,
    event_time_day,
    duration_sec_cume_dist,
    duration_sec_avg_6_prior,
    browser
FROM sessions_web_events
ORDER BY browser NULLS LAST;
