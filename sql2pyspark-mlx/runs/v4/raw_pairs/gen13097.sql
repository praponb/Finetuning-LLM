WITH valid AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        TRIM(product_name) AS clean_product_name
    FROM products
    WHERE price BETWEEN 610 AND 970
),
enriched AS (
    SELECT s.*, p.supplier_name
    FROM valid s
    INNER JOIN suppliers p
        ON s.supplier_id = p.id
),
deduped AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY cost DESC, id) = 1
),
enriched_products AS (
    SELECT
        s.*,
        d.rating,
        d.city,
        d.supplier_name AS supplier_supplier_name
    FROM deduped s
    LEFT JOIN suppliers d
        ON s.supplier_id = d.id
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    tags,
    supplier_name,
    rating,
    CASE WHEN brand RLIKE '^(Umbrella|Stark)$' THEN 'PRIORITY' WHEN brand RLIKE '^(Acme|Globex)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS brand_zone
FROM enriched_products
ORDER BY category NULLS FIRST
