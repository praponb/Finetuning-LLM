WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        duration_sec,
        ROUND(duration_sec * 1.1, 2) AS duration_sec_adjusted,
        COALESCE(page, 'OTHER') AS resolved_page
    FROM web_events
    WHERE session_id IN (2, 8, 32)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(duration_sec) OVER (PARTITION BY event_type) AS event_type_max_duration_sec,
        LEAD(duration_sec, 2) OVER (PARTITION BY session_id ORDER BY duration_sec, id) AS next_duration_sec
    FROM filtered
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec_adjusted DESC, id) = 1
),
deduped_web_events AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS latest_rank
        FROM deduped
    ) x
    WHERE latest_rank = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec_adjusted NULLS FIRST, id) AS dedup_rank
    FROM deduped_web_events
    QUALIFY dedup_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        event_type,
        duration_sec,
        duration_sec_adjusted,
        resolved_page,
        event_type_max_duration_sec,
        next_duration_sec,
        latest_rank,
        dedup_rank,
        SUM(duration_sec) OVER (PARTITION BY session_id) AS session_id_sum_duration_sec,
        MIN(duration_sec) OVER (PARTITION BY session_id) AS session_id_min_duration_sec
    FROM ranked
),
remaining_keys AS (
    -- Set difference on keys
    SELECT session_id
    FROM scored
    EXCEPT ALL
    SELECT session_id
    FROM web_events
    WHERE event_type = 'search'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN remaining_keys k ON s.session_id = k.session_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN session_id_min_duration_sec >= 441 THEN 'HIGH' ELSE 'LOW' END AS session_id_min_duration_sec_tier,
        SUM(CASE WHEN event_type = 'purchase' THEN duration_sec_adjusted ELSE 0 END) AS purchase_duration_sec_adjusted,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN event_type = 'purchase' THEN duration_sec ELSE 0 END) AS purchase_duration_sec,
        PERCENTILE_APPROX(duration_sec_adjusted, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_adjusted_quartiles
    FROM kept_rows
    GROUP BY 1
)
SELECT
    session_id_min_duration_sec_tier,
    purchase_duration_sec_adjusted,
    distinct_ids,
    purchase_duration_sec,
    duration_sec_adjusted_quartiles
FROM summary
ORDER BY purchase_duration_sec;
