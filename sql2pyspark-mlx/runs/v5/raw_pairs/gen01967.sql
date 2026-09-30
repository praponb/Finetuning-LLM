WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_type,
        page,
        duration_sec,
        SUBSTRING(page, 1, 2) AS page_prefix,
        COALESCE(page, 'DIRECT') AS resolved_page
    FROM web_events
    WHERE page IS NOT NULL
      AND event_type != 'add_to_cart'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        COALESCE(page, 'DIRECT') AS resolved_page_2
    FROM filtered
    WHERE duration_sec >= 384
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_type,
        resolved_page_2,
        LAG(duration_sec) OVER (PARTITION BY page, event_type ORDER BY duration_sec, id) AS prev_duration_sec,
        LEAD(duration_sec) OVER (PARTITION BY page, event_type ORDER BY duration_sec, id) AS next_duration_sec,
        LAG(duration_sec_filled) OVER (PARTITION BY page, event_type ORDER BY duration_sec, id) AS prev_duration_sec_filled,
        LEAD(duration_sec_filled) OVER (PARTITION BY page, event_type ORDER BY duration_sec, id) AS next_duration_sec_filled,
        ROW_NUMBER() OVER (PARTITION BY page, event_type ORDER BY duration_sec, id) AS seq_no,
        NTILE(4) OVER (ORDER BY duration_sec_filled, id) AS duration_sec_filled_bucket
    FROM base
    WHERE duration_sec BETWEEN 279 AND 397
),
with_windows_web_events AS (
    -- Ranking and gap features
    SELECT
        id,
        event_type,
        prev_duration_sec,
        seq_no,
        duration_sec_filled_bucket,
        AVG(prev_duration_sec_filled) OVER (PARTITION BY event_type ORDER BY next_duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS prev_duration_sec_filled_avg_3,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY seq_no, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY prev_duration_sec DESC, id) AS seq_no_2
    FROM with_windows
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY event_type ORDER BY seq_no NULLS LAST) AS rank_in_group
    FROM with_windows_web_events
    QUALIFY rank_in_group = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    event_type,
    COUNT(CASE WHEN event_type = 'search' THEN 1 END) AS search_count,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
FROM latest
GROUP BY ROLLUP (CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END, event_type)
ORDER BY search_count NULLS FIRST;
