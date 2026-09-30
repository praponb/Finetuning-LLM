WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        CONCAT_WS('/', store_name, city) AS store_name_city_key,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM stores
    WHERE opened_date >= TO_DATE('2024-03-08')
      AND region IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(sqft, 2, 0) OVER (ORDER BY opened_date, id) AS prev_sqft,
        NTILE(4) OVER (PARTITION BY city, region ORDER BY sqft DESC, id) AS sqft_bucket
    FROM cleaned
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.product_id IN (2, 3, 9, 10)
        )
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region, city ORDER BY sqft DESC NULLS FIRST) AS rn
        FROM not_flagged
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    store_name,
    city,
    opened_date,
    sqft,
    resolved_region,
    prev_sqft,
    rank_in_group,
    rn,
    CASE WHEN city RLIKE '^(Chiang Mai|New York)$' THEN 'GROUP_A' WHEN city RLIKE '^(Bangkok|Singapore)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS city_zone
FROM deduped
ORDER BY sqft DESC;
