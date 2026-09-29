WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(duration_sec) duration_sec_whole,
        COALESCE(page, 'NONE') resolved_page
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-10-06 16:00:00'
      AND session_id = 40
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT page, event_type
    FROM cleaned
    INTERSECT ALL
    SELECT page, event_type
    FROM web_events
    WHERE page = '/product'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.page = k.page AND s.event_type = k.event_type
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        session_id,
        event_type,
        page,
        duration_sec,
        resolved_page,
        COALESCE(duration_sec, 0) duration_sec_filled,
        LENGTH(event_type) event_type_length,
        COALESCE(page, 'N/A') resolved_page_2
    FROM kept_rows
    WHERE duration_sec_whole < 60
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY duration_sec DESC) duration_sec_percent_rank,
        SUM(event_type_length) OVER w_hist event_type_sum_event_type_length,
        STDDEV_SAMP(event_type_length) OVER w_hist event_type_stddev_samp_event_type_length,
        PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC) duration_sec_percent_rank_2,
        CUME_DIST() OVER (PARTITION BY page ORDER BY duration_sec_filled DESC) duration_sec_filled_cume_dist
    FROM base
    WINDOW w_hist AS (PARTITION BY event_type)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY page, event_type ORDER BY duration_sec DESC NULLS FIRST) duration_sec_rank,
        DENSE_RANK() OVER (PARTITION BY page ORDER BY event_type_stddev_samp_event_type_length DESC) event_type_stddev_samp_event_type_length_dense_rank,
        SUM(duration_sec_percent_rank_2) OVER (PARTITION BY page) page_sum_duration_sec_percent_rank_2
    FROM scored
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        d.browser,
        d.country,
        d.device
    FROM metrics a
    JOIN sessions d
        ON a.session_id = d.id
),
base_web_events AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END is_purchase
    FROM with_dims
    WHERE duration_sec <= 380
)
SELECT
    session_id,
    event_type,
    page,
    duration_sec,
    resolved_page,
    duration_sec_filled,
    event_type_length,
    duration_sec_percent_rank,
    event_type_stddev_samp_event_type_length
FROM base_web_events
ORDER BY event_type_stddev_samp_event_type_length ASC NULLS LAST, event_type_length ASC
