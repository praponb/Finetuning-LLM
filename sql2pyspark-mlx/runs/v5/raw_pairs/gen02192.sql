WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN duration_sec >= 399 THEN 'High' WHEN duration_sec >= 196 THEN 'Medium' ELSE 'Low' END AS duration_sec_level
    FROM web_events
    WHERE event_time > DATE '2023-06-14'
      AND page IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        COUNT(duration_sec) OVER (PARTITION BY page, event_type ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY page, event_type ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY page, event_type ORDER BY event_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_duration_sec
    FROM valid
    WHERE session_id = 12
),
enriched_web_events AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(duration_sec, TRUE) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        COUNT(last4_avg_duration_sec) OVER (PARTITION BY page) AS page_count_last4_avg_duration_sec,
        RANK() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS FIRST) AS duration_sec_rank,
        MAX(last4_avg_duration_sec) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS last4_avg_duration_sec_max_30d,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) AS seq_no
    FROM enriched
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(last4_sum_duration_sec, 1) OVER (PARTITION BY page ORDER BY first_known_duration_sec, id) AS prev_last4_sum_duration_sec,
        FIRST_VALUE(page_count_last4_avg_duration_sec, TRUE) OVER (ORDER BY last4_avg_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_page_count_last4_avg_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY last4_avg_duration_sec, id) AS seq_no_2
    FROM enriched_web_events
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(page) AS page_length,
        first_known_duration_sec * duration_sec_rank AS first_known_duration_sec_times_duration_sec_rank,
        COALESCE(page, 'OTHER') AS resolved_page
    FROM scored
    WHERE page IS NOT NULL
      AND event_time >= TIMESTAMP '2023-04-05 05:00:00'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        page,
        duration_sec,
        last4_sum_duration_sec,
        first_known_duration_sec,
        duration_sec_rank,
        seq_no,
        first_known_page_count_last4_avg_duration_sec,
        page_length
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY first_known_duration_sec DESC NULLS FIRST, id) AS rn
        FROM filtered
    ) t
    WHERE rn = 1
),
scored_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(page_length, 1) OVER (PARTITION BY page ORDER BY first_known_duration_sec, id) AS prev_page_length,
        LAG(seq_no, 1) OVER (PARTITION BY page ORDER BY duration_sec_rank, id) AS prev_seq_no,
        LAST_VALUE(page) IGNORE NULLS OVER (PARTITION BY page ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_page
    FROM deduped
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN page_length >= 63 THEN 'LARGE' ELSE 'SMALL' END AS page_length_tier,
    SORT_ARRAY(COLLECT_SET(page)) AS page_list,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    SORT_ARRAY(COLLECT_SET(page)) AS page_list_2
FROM scored_web_events
GROUP BY CASE WHEN page_length >= 63 THEN 'LARGE' ELSE 'SMALL' END;
