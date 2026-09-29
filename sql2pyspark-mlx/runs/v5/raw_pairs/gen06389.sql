WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        ROUND(duration_sec * 0.9, 2) AS duration_sec_adjusted,
        CONCAT(page, ' - ', event_type) AS page_event_type
    FROM web_events
    WHERE event_time >= DATE '2023-07-23'
      AND duration_sec BETWEEN 218 AND 257
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        duration_sec_adjusted,
        page_event_type,
        MONTH(event_time) AS event_time_month,
        TRIM(page) AS clean_page
    FROM cleaned
    WHERE page IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        duration_sec_adjusted - event_time_month AS duration_sec_adjusted_minus_event_time_month,
        CAST(duration_sec AS DOUBLE) AS duration_sec_str,
        COALESCE(page, 'DIRECT') AS resolved_page
    FROM base
    WHERE page IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        page_event_type,
        duration_sec_adjusted_minus_event_time_month
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec_adjusted_minus_event_time_month DESC NULLS FIRST, id) AS latest_rank
        FROM filtered
    ) t
    WHERE latest_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        page,
        duration_sec,
        page_event_type,
        duration_sec_adjusted_minus_event_time_month,
        LAG(duration_sec) OVER w_recent AS prev_duration_sec,
        LAG(duration_sec_adjusted_minus_event_time_month) OVER w_recent AS prev_duration_sec_adjusted_minus_event_time_month,
        LEAD(duration_sec_adjusted_minus_event_time_month) OVER w_recent AS next_duration_sec_adjusted_minus_event_time_month,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM ranked
    WINDOW w_recent AS (PARTITION BY page ORDER BY duration_sec_adjusted_minus_event_time_month, id)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(duration_sec_adjusted_minus_event_time_month) OVER (PARTITION BY page ORDER BY seq_no, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS duration_sec_adjusted_minus_event_time_month_avg_5_prior,
        ROW_NUMBER() OVER w_ordered AS seq_no_2
    FROM scored
    WINDOW w_ordered AS (PARTITION BY page ORDER BY prev_duration_sec_adjusted_minus_event_time_month DESC, id)
),
scored_web_events AS (
    -- Ranking and gap features
    SELECT
        id,
        page,
        page_event_type,
        duration_sec_adjusted_minus_event_time_month,
        prev_duration_sec,
        prev_duration_sec_adjusted_minus_event_time_month,
        AVG(duration_sec_adjusted_minus_event_time_month) OVER (ORDER BY next_duration_sec_adjusted_minus_event_time_month, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS duration_sec_adjusted_minus_event_time_month_avg_5_prior_2,
        AVG(prev_duration_sec_adjusted_minus_event_time_month) OVER (PARTITION BY page ORDER BY duration_sec_adjusted_minus_event_time_month, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS prev_duration_sec_adjusted_minus_event_time_month_avg_3_prior,
        ROW_NUMBER() OVER (ORDER BY prev_duration_sec_adjusted_minus_event_time_month, id) AS row_seq
    FROM enriched
    WHERE prev_duration_sec > 185
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    page,
    metric_name,
    reading
FROM (
    SELECT
        id,
        page,
        prev_duration_sec_adjusted_minus_event_time_month,
        duration_sec_adjusted_minus_event_time_month
    FROM scored_web_events
) src
UNPIVOT (
    reading FOR metric_name IN (prev_duration_sec_adjusted_minus_event_time_month, duration_sec_adjusted_minus_event_time_month)
) long_rows;
