WITH valid AS (
    SELECT *, TO_DATE(event_time) AS event_time_date
    FROM web_events
    WHERE event_time >= '2024-06-01'
),
with_dims AS (
    SELECT
        e.*,
        s.device,
        s.browser,
        s.country
    FROM valid e
    LEFT JOIN sessions s
        ON e.session_id = s.id
),
scored AS (
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        browser,
        LEAD(duration_sec) OVER w_hist AS next_duration_sec,
        LAG(duration_sec) OVER w_hist AS prev_duration_sec,
        ROW_NUMBER() OVER w_hist AS seq_no,
        AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_2_prior
    FROM with_dims
    WHERE event_type = 'add_to_cart'
    WINDOW w_hist AS (PARTITION BY country ORDER BY event_time_date ASC, id ASC)
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY browser ORDER BY duration_sec DESC) AS row_num
        FROM scored
    ) x
    WHERE row_num = 1
),
deduped AS (
    SELECT
        id,
        event_type,
        next_duration_sec,
        prev_duration_sec,
        seq_no,
        duration_sec_avg_2_prior,
        row_num
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY next_duration_sec ASC, id ASC) = 1
),
summary AS (
    SELECT
        event_type,
        SIZE(COLLECT_SET(event_type)) AS num_event_types,
        COUNT(CASE WHEN event_type = 'click' THEN 1 END) AS click_count,
        PERCENTILE_APPROX(next_duration_sec, 0.9) AS next_duration_sec_p90,
        COUNT(*) FILTER (WHERE event_type = 'search') AS search_rows
    FROM deduped
    GROUP BY event_type
)
SELECT
    event_type,
    num_event_types,
    next_duration_sec_p90,
    search_rows
FROM summary;
