WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        delivered_date,
        cost,
        status,
        DATE_ADD(delivered_date, 19) AS delivered_date_plus_19d
    FROM shipments
    WHERE carrier IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        carrier,
        delivered_date_plus_19d
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date_plus_19d NULLS LAST, id) AS dedup_rank
        FROM cleaned
    ) ranked
    WHERE dedup_rank = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date_plus_19d DESC, id) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
ranked_shipments AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY latest_rank, id) = 1
),
ranked_shipments_2 AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY carrier ORDER BY latest_rank DESC) AS row_num
    FROM ranked_shipments
    QUALIFY row_num = 1
)
-- Multi-level aggregation
SELECT
    carrier,
    DATE_TRUNC('QUARTER', delivered_date_plus_19d) AS quarter_start,
    COUNT(DISTINCT carrier) AS distinct_carriers,
    SIZE(COLLECT_SET(carrier)) AS num_carriers,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list,
    COUNT(*) AS row_count,
    SIZE(COLLECT_SET(carrier)) AS num_carriers_2,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list_2
FROM ranked_shipments_2
GROUP BY carrier, DATE_TRUNC('QUARTER', delivered_date_plus_19d)
HAVING num_carriers > 1
ORDER BY quarter_start DESC NULLS FIRST
