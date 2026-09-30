WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        HOUR(started_at) AS started_at_hour
    FROM sessions
    WHERE browser IS DISTINCT FROM 'Chrome'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        started_at_hour,
        COUNT(started_at_hour) OVER (PARTITION BY customer_id ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS started_at_hour_count_30d,
        AVG(started_at_hour) OVER (PARTITION BY browser ORDER BY started_at_hour, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS started_at_hour_avg_2,
        LAG(started_at_hour, 1) OVER w_recent AS prev_started_at_hour
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY device, browser ORDER BY started_at, id)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        started_at_hour,
        prev_started_at_hour,
        CUME_DIST() OVER (ORDER BY started_at_hour_count_30d NULLS FIRST) AS started_at_hour_count_30d_cume_dist,
        AVG(started_at_hour_avg_2) OVER (PARTITION BY customer_id ORDER BY prev_started_at_hour, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS started_at_hour_avg_2_avg_6_prior,
        AVG(started_at_hour) OVER (PARTITION BY customer_id ORDER BY started_at_hour_avg_2, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS started_at_hour_avg_6_prior
    FROM with_windows
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.started_at_hour,
        s.prev_started_at_hour,
        s.started_at_hour_count_30d_cume_dist,
        s.started_at_hour_avg_2_avg_6_prior,
        top1.page,
        top1.event_type
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.page, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    started_at_hour,
    prev_started_at_hour,
    started_at_hour_count_30d_cume_dist,
    started_at_hour_avg_2_avg_6_prior,
    page
FROM latest_match;
