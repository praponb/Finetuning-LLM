WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ARRAY_CONTAINS(tags, 'bestseller') AS is_bestseller
    FROM products
    WHERE brand IS NOT NULL
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        d.supplier_name,
        d.country,
        d.city
    FROM filtered src
    LEFT JOIN suppliers d
        ON src.supplier_id = d.id
),
enriched AS (
    -- Attach suppliers attributes
    SELECT
        src.id,
        src.product_name,
        src.brand,
        src.price,
        src.cost,
        src.launch_date,
        src.supplier_id,
        src.is_bestseller,
        src.city,
        s.rating
    FROM with_dims src
    LEFT JOIN suppliers s
        ON src.supplier_id = s.id
),
enriched_products AS (
    -- Attach suppliers attributes
    SELECT
        s.id,
        s.product_name,
        s.brand,
        s.supplier_id,
        s.is_bestseller,
        b.supplier_name,
        b.country,
        b.city AS supplier_city
    FROM enriched s
    LEFT JOIN suppliers b
        ON s.supplier_id = b.id
),
enriched_products_2 AS (
    -- Attach suppliers attributes
    SELECT e.*, p.city
    FROM enriched_products e
    INNER JOIN suppliers p
        ON e.supplier_id <=> p.id
)
SELECT
    id,
    brand,
    supplier_id,
    is_bestseller,
    country,
    city,
    CASE WHEN country RLIKE '^(DE|TH)$' THEN 'GROUP_B' WHEN country RLIKE '^(US|SG)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS country_zone
FROM enriched_products_2;
