WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        event_type,
        page,
        event_time,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        NTILE(4) OVER (PARTITION BY event_type ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
        NTILE(3) OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS duration_sec_bucket_2
    FROM web_events
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        event_type,
        page,
        last_known_duration_sec,
        duration_sec_bucket_2,
        AVG(duration_sec_bucket) OVER (ORDER BY event_time, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS duration_sec_bucket_avg_5_prior,
        ROUND(duration_sec_bucket_2 / SUM(duration_sec_bucket_2) OVER (PARTITION BY page, event_type), 4) AS duration_sec_bucket_2_share
    FROM enriched
),
enriched_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(last_known_duration_sec) OVER (PARTITION BY event_type) AS event_type_sum_last_known_duration_sec,
        CUME_DIST() OVER (PARTITION BY page, event_type ORDER BY duration_sec_bucket_avg_5_prior NULLS LAST) AS duration_sec_bucket_avg_5_prior_cume_dist,
        SUM(duration_sec_bucket_avg_5_prior) OVER (PARTITION BY page, event_type ORDER BY duration_sec_bucket_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_bucket_avg_5_prior
    FROM with_windows
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec_bucket_avg_5_prior DESC, id) AS rank_in_group
        FROM enriched_web_events
    ) x
    WHERE rank_in_group = 1
)
SELECT
    id,
    page,
    last_known_duration_sec,
    duration_sec_bucket_2,
    duration_sec_bucket_avg_5_prior,
    duration_sec_bucket_2_share,
    event_type_sum_last_known_duration_sec,
    duration_sec_bucket_avg_5_prior_cume_dist,
    cumulative_duration_sec_bucket_avg_5_prior
FROM latest
ORDER BY id DESC NULLS FIRST
