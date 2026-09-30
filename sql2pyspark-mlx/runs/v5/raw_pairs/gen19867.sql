WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        event_type,
        page,
        duration_sec,
        UPPER(event_type) AS event_type_upper,
        SUBSTRING(event_type, 1, 3) AS event_type_prefix
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-08-30 11:00:00'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        event_type,
        page,
        duration_sec,
        event_type_prefix,
        SUM(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        AVG(duration_sec) OVER w_part AS duration_sec_avg_5_prior,
        SUM(duration_sec) OVER (PARTITION BY page, event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_2,
        PERCENT_RANK() OVER (ORDER BY duration_sec) AS duration_sec_percent_rank
    FROM filtered
    WHERE event_type = 'purchase'
    WINDOW w_part AS (ORDER BY duration_sec, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        event_type,
        duration_sec_avg_5_prior,
        duration_sec_percent_rank,
        NTILE(4) OVER w_running AS duration_sec_avg_5_prior_bucket,
        SUM(duration_sec_percent_rank) OVER (PARTITION BY page ORDER BY running_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_percent_rank,
        AVG(duration_sec) OVER (PARTITION BY page, event_type ORDER BY duration_sec_percent_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_3_prior,
        LEAD(running_duration_sec, 1) OVER (PARTITION BY event_type, page ORDER BY duration_sec, id) AS next_running_duration_sec,
        MIN(running_duration_sec_2) OVER (PARTITION BY event_type, page) AS event_type_min_running_duration_sec_2
    FROM with_windows
    WINDOW w_running AS (PARTITION BY page ORDER BY duration_sec_avg_5_prior, id)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT event_type, id
    FROM enriched
    INTERSECT ALL
    SELECT event_type, id
    FROM web_events
    WHERE duration_sec > 252
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.event_type = k.event_type AND s.id = k.id
),
filtered_web_events AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, duration_sec_avg_5_prior_bucket * duration_sec_percent_rank AS duration_sec_avg_5_prior_bucket_times_duration_sec_percent_rank
    FROM kept_rows
    WHERE next_running_duration_sec <> 1131
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN event_type_min_running_duration_sec_2 >= 2341 THEN 'HIGH' ELSE 'LOW' END AS event_type_min_running_duration_sec_2_tier,
        event_type,
        APPROX_PERCENTILE(duration_sec_percent_rank, 0.9) AS duration_sec_percent_rank_p90,
        AVG(duration_sec_percent_rank) AS avg_duration_sec_percent_rank,
        MAX(duration_sec_avg_5_prior_bucket) FILTER (WHERE event_type = 'page_view') AS page_view_max_duration_sec_avg_5_prior_bucket
    FROM filtered_web_events
    GROUP BY CASE WHEN event_type_min_running_duration_sec_2 >= 2341 THEN 'HIGH' ELSE 'LOW' END, event_type
    HAVING MAX(duration_sec_avg_5_prior_bucket) FILTER (WHERE event_type = 'page_view') < 1000000
)
SELECT
    event_type_min_running_duration_sec_2_tier,
    event_type,
    avg_duration_sec_percent_rank,
    page_view_max_duration_sec_avg_5_prior_bucket
FROM grouped
ORDER BY event_type DESC NULLS FIRST
