WITH base AS (
    SELECT
        *,
        INITCAP(TRIM(region)) AS region_title,
        CONCAT(region, '/', store_name) AS region_store_name
    FROM stores
    WHERE region IS NOT NULL
),
base_stores AS (
    SELECT
        *,
        CONCAT(store_name, ' - ', city) AS store_name_city,
        COALESCE(region, 'N/A') AS resolved_region
    FROM base
    WHERE opened_date >= DATE '2023-08-13'
      AND region IS NOT NULL
),
eligible AS (
    SELECT s.*
    FROM base_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.store_id IN (1, 2, 3)
        )
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS row_num
    FROM eligible
    QUALIFY row_num = 1
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    region_title,
    region_store_name,
    store_name_city
FROM deduped;
