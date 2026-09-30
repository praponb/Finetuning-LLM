WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(category) AS clean_category,
        COALESCE(brand, 'UNKNOWN') AS brand_clean
    FROM products
    WHERE supplier_id IS NOT NULL
      AND UPPER(product_name) LIKE '%FFI%'
),
cleaned_products AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        REPLACE(product_name, ' ', '_') AS product_name_slug,
        COALESCE(brand, 'UNKNOWN') AS brand_filled
    FROM cleaned
    WHERE brand = 'Umbrella'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT category, id
    FROM cleaned_products
    EXCEPT
    SELECT category, id
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'premium')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned_products s
    INNER JOIN remaining_keys k ON s.category = k.category AND s.id = k.id
),
enriched AS (
    -- Attach suppliers attributes
    SELECT a.*, p.city
    FROM verified a
    INNER JOIN suppliers p
        ON a.supplier_id = p.id
)
SELECT
    id,
    category,
    price,
    cost,
    stock_qty,
    tags,
    supplier_id,
    clean_category,
    brand_clean,
    CASE WHEN category RLIKE '^(Sports|Books)$' THEN 'OTHER_ZONE' WHEN category RLIKE '^(Electronics|Home)$' THEN 'PRIORITY' ELSE 'CORE' END AS category_zone
FROM enriched
