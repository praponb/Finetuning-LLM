WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        WEEKOFYEAR(event_time) AS event_time_weekofyear,
        ROUND(duration_sec, 2) AS duration_sec_rounded,
        COALESCE(page, 'OTHER') AS resolved_page
    FROM web_events
    WHERE event_time > DATE '2023-03-25'
      AND page IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        event_type,
        page,
        event_time,
        event_time_weekofyear,
        resolved_page,
        AVG(event_time_weekofyear) OVER w_recent AS last7_avg_event_time_weekofyear,
        COUNT(event_time_weekofyear) OVER w_recent AS last7_count_event_time_weekofyear,
        SUM(event_time_weekofyear) OVER w_recent AS last7_sum_event_time_weekofyear,
        MAX(event_time_weekofyear) OVER w_recent AS last7_max_event_time_weekofyear,
        LAG(event_time_weekofyear, 1) OVER w_ordered AS prev_event_time_weekofyear
    FROM base
    WHERE TO_DATE(event_time) >= '2024-10-01'
    WINDOW w_recent AS (PARTITION BY session_id, page ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY page ORDER BY event_time_weekofyear, id)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY last7_avg_event_time_weekofyear DESC NULLS FIRST, id) = 1
),
scored_web_events AS (
    -- Ranking and gap features
    SELECT
        id,
        page,
        event_time,
        event_time_weekofyear,
        last7_sum_event_time_weekofyear,
        last7_max_event_time_weekofyear,
        prev_event_time_weekofyear,
        LAST_VALUE(last7_max_event_time_weekofyear) IGNORE NULLS OVER (PARTITION BY page ORDER BY last7_sum_event_time_weekofyear, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last7_max_event_time_weekofyear,
        SUM(last7_avg_event_time_weekofyear) OVER (ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last7_avg_event_time_weekofyear,
        FIRST_VALUE(last7_sum_event_time_weekofyear, TRUE) OVER (PARTITION BY page ORDER BY prev_event_time_weekofyear, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last7_sum_event_time_weekofyear,
        SUM(last7_sum_event_time_weekofyear) OVER (PARTITION BY page, event_type) AS page_sum_last7_sum_event_time_weekofyear,
        RANK() OVER (PARTITION BY page ORDER BY last7_count_event_time_weekofyear DESC NULLS FIRST) AS last7_count_event_time_weekofyear_rank
    FROM deduped
),
scored_web_events_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY page ORDER BY last7_count_event_time_weekofyear_rank, id) AS last7_count_event_time_weekofyear_rank_bucket,
        LAG(last7_count_event_time_weekofyear_rank, 1, 0) OVER w_ordered AS prev_last7_count_event_time_weekofyear_rank
    FROM scored_web_events
    WINDOW w_ordered AS (PARTITION BY page ORDER BY event_time_weekofyear, id)
),
daily AS (
    -- Multi-level aggregation
    SELECT
        page,
        ROUND(SUM(event_time_weekofyear) / NULLIF(COUNT(CASE WHEN page = '/checkout' THEN 1 END), 0), 2) AS event_time_weekofyear_per_checkout,
        AVG(last7_count_event_time_weekofyear_rank) AS avg_last7_count_event_time_weekofyear_rank,
        SUM(CASE WHEN page = '/cart' THEN event_time_weekofyear ELSE 0 END) AS cart_event_time_weekofyear,
        ROUND(SUM(last7_max_event_time_weekofyear) / NULLIF(COUNT(CASE WHEN page = '/search' THEN 1 END), 0), 2) AS last7_max_event_time_weekofyear_per_search,
        COUNT(DISTINCT page) AS distinct_pages,
        COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count
    FROM scored_web_events_2
    GROUP BY page
    HAVING distinct_pages >= 1
)
SELECT
    page,
    event_time_weekofyear_per_checkout,
    cart_event_time_weekofyear,
    last7_max_event_time_weekofyear_per_search,
    distinct_pages,
    checkout_count
FROM daily;
