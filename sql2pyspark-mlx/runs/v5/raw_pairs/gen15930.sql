WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        region,
        LENGTH(region) AS region_length,
        SUBSTRING(region, 1, 3) AS region_prefix
    FROM stores
    WHERE opened_date >= DATE '2023-09-17'
      AND region IS NOT DISTINCT FROM 'Central'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND MONTH(ch.sale_date) IN (2, 4, 5)
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region ORDER BY region_length DESC NULLS LAST) AS latest_rank
        FROM eligible
    ) x
    WHERE latest_rank = 1
),
deduped_stores AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY latest_rank ASC, id ASC) AS rn
    FROM deduped
    QUALIFY rn = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped_stores s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.order_date <= '2023-09-01'
        )
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY region ORDER BY rn DESC) AS latest_rank_2
    FROM kept
    QUALIFY latest_rank_2 = 1
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN region_length >= 73 THEN 'HIGH' ELSE 'LOW' END AS region_length_tier,
        SUM(CASE WHEN region = 'North' THEN region_length ELSE 0 END) AS north_region_length,
        COUNT(DISTINCT id) AS distinct_ids,
        SORT_ARRAY(COLLECT_SET(region)) AS region_list
    FROM latest
    GROUP BY 1
)
SELECT
    region_length_tier,
    distinct_ids,
    region_list
FROM daily
