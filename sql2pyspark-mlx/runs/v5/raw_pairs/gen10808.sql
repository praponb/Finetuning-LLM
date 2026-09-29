WITH remaining_keys AS (
    -- Set difference on keys
    SELECT store_id, product_id
    FROM store_sales
    EXCEPT
    SELECT store_id, product_id
    FROM store_sales
    WHERE units <= 19
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    INNER JOIN remaining_keys k ON s.store_id = k.store_id AND s.product_id = k.product_id
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.*,
        s.opened_date,
        s.sqft,
        s.region
    FROM verified src
    INNER JOIN stores s
        ON src.store_id = s.id
),
remaining_keys_2 AS (
    -- Set difference on keys
    SELECT product_id, store_id
    FROM joined
    EXCEPT
    SELECT product_id, store_id
    FROM store_sales
    WHERE units > 22
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN remaining_keys_2 k ON s.product_id = k.product_id AND s.store_id = k.store_id
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.store_id,
        src.product_id,
        src.sale_date,
        src.units,
        src.sqft,
        s.store_name,
        s.city,
        s.region AS store_region
    FROM reconciled src
    LEFT JOIN stores s
        ON src.store_id = s.id
)
SELECT
    s.store_id,
    s.sale_date,
    s.units,
    s.store_name,
    s.city,
    s.store_region,
    CASE WHEN store_region RLIKE '^(South|West)$' THEN 'GROUP_B' ELSE 'CORE' END AS store_region_zone,
    (SELECT SUM(s2.units) FROM store_sales s2 WHERE s2.store_id = s.store_id) AS sum_store_sale_units
FROM with_dims s
ORDER BY city DESC NULLS FIRST, sale_date
