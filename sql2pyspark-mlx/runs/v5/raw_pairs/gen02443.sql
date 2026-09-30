WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        page,
        LAG(duration_sec, 1) OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS prev_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        NTILE(10) OVER (PARTITION BY session_id, page ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
        LAST_VALUE(duration_sec, TRUE) OVER (PARTITION BY page, session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS duration_sec_avg_5
    FROM web_events
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec_avg_5 NULLS LAST, id) AS rank_in_group
        FROM scored
    ) x
    WHERE rank_in_group = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT page, id
    FROM ranked
    EXCEPT DISTINCT
    SELECT page, id
    FROM web_events
    WHERE session_id IN (1, 13, 35)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.page = k.page AND s.id = k.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        page,
        prev_duration_sec,
        cumulative_duration_sec,
        duration_sec_bucket,
        duration_sec_avg_5,
        rank_in_group,
        UPPER(page) AS page_upper,
        TRIM(page) AS clean_page,
        COALESCE(page, 'N/A') AS page_clean
    FROM kept_rows
    WHERE page IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        page,
        prev_duration_sec,
        duration_sec_bucket,
        duration_sec_avg_5,
        page_upper,
        clean_page,
        page_clean,
        DENSE_RANK() OVER (ORDER BY duration_sec_avg_5 NULLS FIRST) AS duration_sec_avg_5_dense_rank,
        STDDEV_SAMP(duration_sec_bucket) OVER (PARTITION BY page) AS page_stddev_samp_duration_sec_bucket,
        CUME_DIST() OVER (PARTITION BY page ORDER BY cumulative_duration_sec NULLS LAST) AS cumulative_duration_sec_cume_dist
    FROM valid
    WHERE rank_in_group >= 1
),
metrics_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        prev_duration_sec,
        duration_sec_avg_5,
        clean_page,
        duration_sec_avg_5_dense_rank,
        PERCENT_RANK() OVER (ORDER BY cumulative_duration_sec_cume_dist DESC NULLS FIRST) AS cumulative_duration_sec_cume_dist_percent_rank,
        RANK() OVER w_hist AS duration_sec_bucket_rank,
        CUME_DIST() OVER (PARTITION BY page ORDER BY prev_duration_sec DESC) AS prev_duration_sec_cume_dist
    FROM metrics
    WINDOW w_hist AS (ORDER BY duration_sec_bucket NULLS LAST)
)
SELECT
    prev_duration_sec,
    clean_page,
    duration_sec_avg_5_dense_rank,
    cumulative_duration_sec_cume_dist_percent_rank,
    prev_duration_sec_cume_dist
FROM metrics_web_events
ORDER BY duration_sec_avg_5_dense_rank DESC
