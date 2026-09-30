WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(duration_sec) OVER (PARTITION BY session_id, page ORDER BY duration_sec, id) AS next_duration_sec,
        LAG(duration_sec) OVER (PARTITION BY session_id, page ORDER BY duration_sec, id) AS prev_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY session_id, page ORDER BY duration_sec, id) AS seq_no
    FROM web_events
    WHERE TO_DATE(event_time) < '2025-08-23'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS LAST) = 1
),
ranked_web_events AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id, page ORDER BY seq_no, id) AS rank_in_group
        FROM ranked
    ) t
    WHERE rank_in_group = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT page
    FROM ranked_web_events
    EXCEPT
    SELECT page
    FROM web_events
    WHERE duration_sec BETWEEN 150 AND 376
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked_web_events s
    INNER JOIN kept_keys k ON s.page = k.page
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY page ORDER BY next_duration_sec NULLS FIRST) AS next_duration_sec_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY session_id, page ORDER BY duration_sec DESC NULLS FIRST) AS duration_sec_percent_rank
    FROM reconciled
),
kept_keys_2 AS (
    -- Set intersection on keys
    SELECT session_id, page
    FROM scored
    INTERSECT ALL
    SELECT session_id, page
    FROM web_events
    WHERE session_id IN (5, 8, 17)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys_2 k ON s.session_id = k.session_id AND s.page = k.page
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            event_type,
            page,
            duration_sec_percent_rank
        FROM kept_rows
    ) src
    PIVOT (
        MAX(duration_sec_percent_rank) AS value_sum,
        COUNT(1) AS n
        FOR page IN ('/home' AS home, '/product' AS product)
    )
)
SELECT
    event_type,
    home_value_sum,
    home_n,
    product_value_sum,
    product_n,
    CASE WHEN event_type RLIKE '^(page_view|add_to_cart)$' THEN 'OTHER_ZONE' WHEN event_type RLIKE '^(click|purchase)$' THEN 'GROUP_A' ELSE 'CORE' END AS event_type_zone
FROM by_type;
