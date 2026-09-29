WITH scored AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
        COUNT(duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_count_30d,
        ROW_NUMBER() OVER (PARTITION BY page, session_id ORDER BY duration_sec DESC, id) AS seq_no
    FROM web_events
),
with_dims AS (
    SELECT
        e.*,
        p.browser,
        p.country,
        p.device
    FROM scored e
    LEFT JOIN sessions p
        ON e.session_id = p.id
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY country, event_type ORDER BY duration_sec DESC NULLS FIRST) AS latest_rank
    FROM with_dims
    QUALIFY latest_rank = 1
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY device, session_id ORDER BY duration_sec_bucket) AS duration_sec_bucket_dense_rank,
        STDDEV_SAMP(seq_no) OVER (PARTITION BY session_id, event_type) AS session_id_stddev_samp_seq_no
    FROM ranked
),
scored_web_events AS (
    SELECT
        *,
        COUNT(seq_no) OVER (PARTITION BY country) AS country_count_seq_no,
        MAX(duration_sec_bucket) OVER (PARTITION BY event_type, page) AS event_type_max_duration_sec_bucket
    FROM with_windows
),
metrics AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        duration_sec_bucket,
        duration_sec_count_30d,
        seq_no,
        browser,
        country,
        device,
        latest_rank,
        duration_sec_bucket_dense_rank,
        session_id_stddev_samp_seq_no,
        country_count_seq_no,
        event_type_max_duration_sec_bucket,
        LAST_VALUE(duration_sec_count_30d, TRUE) OVER w_running AS last_known_duration_sec_count_30d,
        ROUND(duration_sec_bucket_dense_rank / SUM(duration_sec_bucket_dense_rank) OVER (PARTITION BY session_id), 4) AS duration_sec_bucket_dense_rank_share,
        AVG(duration_sec_bucket) OVER (PARTITION BY session_id ORDER BY duration_sec_bucket_dense_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_bucket_avg_6
    FROM scored_web_events
    WINDOW w_running AS (PARTITION BY device ORDER BY duration_sec_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec_count_30d NULLS LAST, id) = 1
),
stats AS (
    SELECT
        DATE_TRUNC('MONTH', event_time) AS activity_month,
        DATE_TRUNC('YEAR', event_time) AS year_start,
        SIZE(COLLECT_SET(page)) AS num_pages,
        PERCENTILE_APPROX(country_count_seq_no, 0.9) AS country_count_seq_no_p90,
        MAX(event_time) AS ended_at,
        (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
        SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
    FROM latest
    GROUP BY DATE_TRUNC('MONTH', event_time), DATE_TRUNC('YEAR', event_time)
)
SELECT
    activity_month,
    year_start,
    country_count_seq_no_p90,
    span_minutes
FROM stats
ORDER BY year_start;
