WITH matched_keys AS (
    -- Set intersection on keys
    SELECT page, event_type
    FROM web_events
    INTERSECT ALL
    SELECT page, event_type
    FROM web_events
    WHERE duration_sec = 133
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM web_events s
    JOIN matched_keys k ON s.page = k.page AND s.event_type = k.event_type
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(page) AS page_upper,
        COALESCE(page, 'UNKNOWN') AS page_clean
    FROM verified
    WHERE event_time >= TO_DATE('2023-10-18')
),
base AS (
    -- Keep recent, valid rows
    SELECT
        session_id,
        event_type,
        page,
        duration_sec,
        page_upper,
        DATE_TRUNC('MONTH', event_time) AS event_time_period
    FROM filtered
    WHERE event_time >= TO_DATE('2023-09-29')
      AND page IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY duration_sec NULLS LAST) AS duration_sec_dense_rank,
        COUNT(duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_count_30d,
        CUME_DIST() OVER (ORDER BY duration_sec DESC) AS duration_sec_cume_dist,
        PERCENT_RANK() OVER (ORDER BY duration_sec NULLS FIRST) AS duration_sec_percent_rank
    FROM base
)
SELECT
    session_id,
    page,
    duration_sec,
    page_upper,
    event_time_period,
    duration_sec_dense_rank,
    duration_sec_cume_dist,
    duration_sec_percent_rank
FROM metrics
ORDER BY duration_sec_percent_rank NULLS LAST;
