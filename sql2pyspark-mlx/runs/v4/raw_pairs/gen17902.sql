WITH metrics AS (
    SELECT
        *,
        SUM(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS row_seq,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_avg_7d,
        DENSE_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC NULLS FIRST) AS duration_sec_dense_rank
    FROM web_events
),
enriched AS (
    SELECT
        *,
        AVG(cumulative_duration_sec) OVER (ORDER BY duration_sec_dense_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_avg_6,
        LAST_VALUE(duration_sec_avg_7d) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec_avg_7d,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY cumulative_duration_sec, id) AS seq_no,
        AVG(duration_sec_dense_rank) OVER (PARTITION BY page ORDER BY duration_sec_avg_7d, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS duration_sec_dense_rank_avg_5
    FROM metrics
),
enriched_web_events AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        cumulative_duration_sec,
        row_seq,
        first_known_duration_sec,
        duration_sec_avg_7d,
        duration_sec_dense_rank,
        cumulative_duration_sec_avg_6,
        last_known_duration_sec_avg_7d,
        seq_no,
        duration_sec_dense_rank_avg_5,
        MAX(duration_sec) OVER (PARTITION BY session_id) AS session_id_max_duration_sec,
        LEAD(cumulative_duration_sec, 1) OVER (PARTITION BY session_id ORDER BY event_time, id) AS next_cumulative_duration_sec,
        RANK() OVER (ORDER BY duration_sec_avg_7d DESC) AS duration_sec_avg_7d_rank,
        PERCENT_RANK() OVER (PARTITION BY session_id ORDER BY last_known_duration_sec_avg_7d DESC) AS last_known_duration_sec_avg_7d_percent_rank,
        SUM(duration_sec_dense_rank_avg_5) OVER (ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_dense_rank_avg_5
    FROM enriched
),
joined AS (
    SELECT
        e.*,
        p.device,
        p.country
    FROM enriched_web_events e
    JOIN sessions p
        ON e.session_id = p.id
)
SELECT
    id,
    page,
    event_time,
    first_known_duration_sec,
    duration_sec_dense_rank,
    cumulative_duration_sec_avg_6,
    seq_no,
    duration_sec_dense_rank_avg_5,
    session_id_max_duration_sec
FROM joined
ORDER BY page NULLS FIRST, duration_sec_dense_rank_avg_5 DESC;
