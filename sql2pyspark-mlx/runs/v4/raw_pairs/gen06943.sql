WITH cleaned AS (
    SELECT *, CONCAT_WS('-', event_type, page) AS event_type_page_key
    FROM web_events
    WHERE page = '/checkout'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS row_num
        FROM cleaned
    ) x
    WHERE row_num = 1
),
with_dims AS (
    SELECT
        s.*,
        d.device,
        d.browser
    FROM ranked s
    LEFT JOIN sessions d
        ON s.session_id = d.id
),
with_windows AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        event_type_page_key,
        row_num,
        device,
        browser,
        SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_sum_7d,
        DENSE_RANK() OVER w_recent AS duration_sec_dense_rank,
        AVG(duration_sec) OVER w_ordered AS duration_sec_avg_2
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY device ORDER BY duration_sec NULLS FIRST), w_ordered AS (PARTITION BY page ORDER BY row_num, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY page, browser ORDER BY row_num, id) AS row_seq,
        AVG(duration_sec_sum_7d) OVER w_grp AS duration_sec_sum_7d_avg_3,
        LAST_VALUE(device, TRUE) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_device,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY duration_sec_dense_rank, id) AS row_seq_2,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM with_windows
    WINDOW w_grp AS (ORDER BY duration_sec_dense_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY event_type ORDER BY duration_sec, id)
),
with_windows_web_events AS (
    SELECT
        *,
        LAG(duration_sec_sum_7d_avg_3, 1, 0) OVER (PARTITION BY device ORDER BY event_time, id) AS prev_duration_sec_sum_7d_avg_3,
        PERCENT_RANK() OVER (PARTITION BY device ORDER BY duration_sec_avg_2 NULLS LAST) AS duration_sec_avg_2_percent_rank
    FROM metrics
),
latest AS (
    SELECT *
    FROM with_windows_web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec_dense_rank, id) = 1
)
SELECT
    CASE WHEN duration_sec >= 537 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    DATE_TRUNC('WEEK', event_time) AS week_start,
    PERCENTILE_APPROX(duration_sec_sum_7d_avg_3, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_sum_7d_avg_3_quartiles,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    ROUND(AVG(duration_sec_dense_rank), 2) AS avg_duration_sec_dense_rank,
    SIZE(COLLECT_SET(page)) AS num_pages,
    MAX(event_time) AS ended_at
FROM latest
GROUP BY duration_sec_tier, week_start
HAVING SIZE(COLLECT_SET(page)) >= 1
ORDER BY duration_sec_bit_and DESC
