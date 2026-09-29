WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(page) AS page_upper,
        COALESCE(page, 'DIRECT') AS page_clean
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-05-06 20:00:00'
      AND session_id IN (10, 14, 15)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        page_clean,
        DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time
    FROM filtered
    WHERE event_time >= DATE '2023-08-09'
      AND page IS NOT DISTINCT FROM '/checkout'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        page_clean,
        AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_6_prior,
        SUM(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec
    FROM base
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_time,
        page_clean,
        duration_sec_avg_6_prior,
        AVG(duration_sec_avg_6_prior) OVER (PARTITION BY page ORDER BY duration_sec_avg_6_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_avg_6_prior_avg_3,
        PERCENT_RANK() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS FIRST) AS duration_sec_percent_rank,
        SUM(duration_sec_avg_6_prior) OVER w_ordered AS page_sum_duration_sec_avg_6_prior
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY page)
),
with_windows_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(duration_sec_avg_6_prior_avg_3) OVER (ORDER BY page_sum_duration_sec_avg_6_prior, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_6_prior_avg_3_avg_6_prior,
        ROW_NUMBER() OVER (ORDER BY duration_sec_avg_6_prior DESC, id) AS row_seq,
        NTILE(5) OVER (ORDER BY duration_sec_percent_rank, id) AS duration_sec_percent_rank_bucket,
        LAG(page_sum_duration_sec_avg_6_prior, 2, 0) OVER (ORDER BY duration_sec_avg_6_prior, id) AS prev_page_sum_duration_sec_avg_6_prior
    FROM scored
),
filtered_web_events AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        event_time,
        duration_sec_avg_6_prior_avg_3,
        page_sum_duration_sec_avg_6_prior,
        duration_sec_avg_6_prior_avg_3_avg_6_prior,
        row_seq,
        duration_sec_percent_rank_bucket,
        prev_page_sum_duration_sec_avg_6_prior,
        LEAST(duration_sec_avg_6_prior, page_sum_duration_sec_avg_6_prior) AS least_duration_sec_avg_6_prior_page_sum_duration_sec_avg_6_prior,
        ROUND(duration_sec_percent_rank * 0.9, 2) AS duration_sec_percent_rank_scaled
    FROM with_windows_web_events
    WHERE event_time >= TIMESTAMP '2023-05-17 01:00:00'
)
SELECT
    id,
    duration_sec_avg_6_prior_avg_3,
    page_sum_duration_sec_avg_6_prior,
    duration_sec_avg_6_prior_avg_3_avg_6_prior,
    duration_sec_percent_rank_bucket,
    prev_page_sum_duration_sec_avg_6_prior,
    least_duration_sec_avg_6_prior_page_sum_duration_sec_avg_6_prior,
    duration_sec_percent_rank_scaled
FROM filtered_web_events;
