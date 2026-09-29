WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        CAST(duration_sec AS STRING) AS duration_sec_str,
        COALESCE(page, 'N/A') AS page_filled
    FROM web_events
    WHERE event_time > TO_TIMESTAMP('2023-07-19 18:00:00')
      AND event_type = 'purchase'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_type,
        page,
        duration_sec,
        duration_sec_str,
        page_filled,
        AVG(duration_sec) OVER (PARTITION BY event_type, page ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_6_prior,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        NTILE(3) OVER (ORDER BY duration_sec, id) AS duration_sec_bucket,
        STDDEV_SAMP(duration_sec) OVER w_part AS session_id_stddev_samp_duration_sec
    FROM cleaned
    WHERE duration_sec >= 248
    WINDOW w_part AS (PARTITION BY session_id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        duration_sec_str,
        page_filled,
        first_known_duration_sec,
        duration_sec_bucket,
        session_id_stddev_samp_duration_sec,
        LAST_VALUE(session_id_stddev_samp_duration_sec, TRUE) OVER (ORDER BY first_known_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_id_stddev_samp_duration_sec,
        SUM(duration_sec_avg_6_prior) OVER (PARTITION BY event_type ORDER BY duration_sec_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_avg_6_prior
    FROM scored
),
metrics_web_events AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(last_known_session_id_stddev_samp_duration_sec) OVER w_part AS running_count_last_known_session_id_stddev_samp_duration_sec,
        AVG(last_known_session_id_stddev_samp_duration_sec) OVER w_part AS running_avg_last_known_session_id_stddev_samp_duration_sec,
        SUM(last_known_session_id_stddev_samp_duration_sec) OVER w_part AS running_sum_last_known_session_id_stddev_samp_duration_sec
    FROM metrics
    WINDOW w_part AS (ORDER BY session_id_stddev_samp_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LEAST(running_avg_last_known_session_id_stddev_samp_duration_sec, running_sum_last_known_session_id_stddev_samp_duration_sec) AS least_running_avg_last_known_session_id_stddev_samp_duration_sec_running_sum_last_known_session_id_stddev_samp_duration_sec,
        COALESCE(last_known_session_id_stddev_samp_duration_sec, 0) AS last_known_session_id_stddev_samp_duration_sec_filled
    FROM metrics_web_events
    WHERE running_sum_last_known_session_id_stddev_samp_duration_sec <= 600
),
scored_web_events AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(least_running_avg_last_known_session_id_stddev_samp_duration_sec_running_sum_last_known_session_id_stddev_samp_duration_sec) OVER (ORDER BY first_known_duration_sec, id) AS prev_least_running_avg_last_known_session_id_stddev_samp_duration_sec_running_sum_last_known_session_id_stddev_samp_duration_sec,
        LEAD(running_duration_sec_avg_6_prior) OVER (ORDER BY first_known_duration_sec, id) AS next_running_duration_sec_avg_6_prior,
        LAG(running_duration_sec_avg_6_prior) OVER (ORDER BY first_known_duration_sec, id) AS prev_running_duration_sec_avg_6_prior,
        ROW_NUMBER() OVER (ORDER BY first_known_duration_sec, id) AS event_seq,
        FIRST_VALUE(first_known_duration_sec, TRUE) OVER (ORDER BY running_avg_last_known_session_id_stddev_samp_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_duration_sec
    FROM filtered
),
filtered_web_events AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        duration_sec_str,
        page_filled,
        first_known_duration_sec,
        duration_sec_bucket,
        running_duration_sec_avg_6_prior,
        running_avg_last_known_session_id_stddev_samp_duration_sec,
        running_sum_last_known_session_id_stddev_samp_duration_sec,
        last_known_session_id_stddev_samp_duration_sec_filled,
        next_running_duration_sec_avg_6_prior,
        prev_running_duration_sec_avg_6_prior,
        first_known_first_known_duration_sec,
        CASE WHEN running_sum_last_known_session_id_stddev_samp_duration_sec >= 2280 THEN 'High' WHEN running_sum_last_known_session_id_stddev_samp_duration_sec >= 310 THEN 'Medium' ELSE 'Low' END AS running_sum_last_known_session_id_stddev_samp_duration_sec_level
    FROM scored_web_events
    WHERE duration_sec_bucket BETWEEN 3 AND 6
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN running_avg_last_known_session_id_stddev_samp_duration_sec >= 370 THEN 'LARGE' ELSE 'SMALL' END AS running_avg_last_known_session_id_stddev_samp_duration_sec_tier,
        ROUND(AVG(next_running_duration_sec_avg_6_prior), 2) AS avg_next_running_duration_sec_avg_6_prior,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt
    FROM filtered_web_events
    GROUP BY 1
)
SELECT running_avg_last_known_session_id_stddev_samp_duration_sec_tier
FROM summary
ORDER BY running_avg_last_known_session_id_stddev_samp_duration_sec_tier;
