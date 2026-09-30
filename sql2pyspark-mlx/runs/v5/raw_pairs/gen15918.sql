WITH with_windows AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        MIN(duration_sec) OVER w_recent AS last4_min_duration_sec,
        COUNT(duration_sec) OVER w_recent AS last4_count_duration_sec,
        SUM(duration_sec) OVER w_recent AS last4_sum_duration_sec
    FROM web_events
    WINDOW w_recent AS (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        id,
        session_id,
        event_type,
        last4_min_duration_sec,
        last4_count_duration_sec,
        last4_sum_duration_sec,
        NTILE(10) OVER (ORDER BY last4_sum_duration_sec DESC, id) AS last4_sum_duration_sec_bucket,
        DENSE_RANK() OVER (PARTITION BY page ORDER BY duration_sec NULLS LAST) AS duration_sec_dense_rank
    FROM with_windows
    WHERE page IS NOT NULL
),
enriched AS (
    SELECT
        id,
        session_id,
        event_type,
        last4_min_duration_sec,
        duration_sec_dense_rank,
        ROUND(duration_sec_dense_rank / SUM(duration_sec_dense_rank) OVER (PARTITION BY session_id), 4) AS duration_sec_dense_rank_share,
        LEAD(last4_sum_duration_sec_bucket, 1) OVER (PARTITION BY session_id, event_type ORDER BY duration_sec_dense_rank, id) AS next_last4_sum_duration_sec_bucket,
        AVG(last4_min_duration_sec) OVER w_recent AS event_type_avg_last4_min_duration_sec
    FROM metrics
    WINDOW w_recent AS (PARTITION BY event_type)
),
cleaned AS (
    SELECT
        id,
        last4_min_duration_sec,
        duration_sec_dense_rank,
        duration_sec_dense_rank_share,
        next_last4_sum_duration_sec_bucket,
        UPPER(event_type) AS event_type_upper
    FROM enriched
    WHERE last4_min_duration_sec > 230
)
SELECT
    CASE WHEN last4_min_duration_sec >= 510 THEN 'LARGE' ELSE 'SMALL' END AS last4_min_duration_sec_tier,
    CASE WHEN duration_sec_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_dense_rank_tier,
    COUNT(*) AS row_count,
    ROUND(AVG(duration_sec_dense_rank), 2) AS avg_duration_sec_dense_rank,
    PERCENTILE_APPROX(last4_min_duration_sec, ARRAY(0.25, 0.5, 0.75)) AS last4_min_duration_sec_quartiles
FROM cleaned
GROUP BY CUBE (CASE WHEN last4_min_duration_sec >= 510 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN duration_sec_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END);
