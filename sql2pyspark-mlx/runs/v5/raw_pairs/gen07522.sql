WITH joined AS (
    -- Attach sessions attributes
    SELECT a.*, p.browser
    FROM web_events a
    LEFT JOIN sessions p
        ON a.session_id = p.id
    WHERE p.customer_id = 39
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        event_type,
        duration_sec
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type, browser ORDER BY duration_sec ASC, id ASC) AS row_num
        FROM joined
    ) x
    WHERE row_num = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT id, duration_sec
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id ASC) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        duration_sec,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        DENSE_RANK() OVER (ORDER BY duration_sec ASC NULLS FIRST) AS duration_sec_dense_rank,
        LAG(duration_sec, 1) OVER (ORDER BY duration_sec ASC, id ASC) AS prev_duration_sec
    FROM latest
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        duration_sec,
        last_known_duration_sec,
        duration_sec_dense_rank,
        SUM(duration_sec_dense_rank) OVER (ORDER BY last_known_duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_dense_rank,
        AVG(last_known_duration_sec) OVER (ORDER BY duration_sec_dense_rank ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS last_known_duration_sec_avg_6_prior,
        SUM(duration_sec_dense_rank) OVER (ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_dense_rank_2
    FROM with_windows
),
metrics_web_events AS (
    -- Ranking and gap features
    SELECT
        id,
        duration_sec_dense_rank,
        last_known_duration_sec_avg_6_prior,
        cumulative_duration_sec_dense_rank_2,
        MAX(last_known_duration_sec_avg_6_prior) OVER w_grp AS running_max_last_known_duration_sec_avg_6_prior,
        SUM(last_known_duration_sec_avg_6_prior) OVER w_grp AS running_sum_last_known_duration_sec_avg_6_prior,
        COUNT(last_known_duration_sec_avg_6_prior) OVER w_grp AS running_count_last_known_duration_sec_avg_6_prior,
        AVG(last_known_duration_sec_avg_6_prior) OVER w_grp AS running_avg_last_known_duration_sec_avg_6_prior
    FROM metrics
    WINDOW w_grp AS (ORDER BY cumulative_duration_sec_dense_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    duration_sec_dense_rank,
    cumulative_duration_sec_dense_rank_2,
    running_avg_last_known_duration_sec_avg_6_prior
FROM metrics_web_events
ORDER BY cumulative_duration_sec_dense_rank_2 ASC NULLS LAST
