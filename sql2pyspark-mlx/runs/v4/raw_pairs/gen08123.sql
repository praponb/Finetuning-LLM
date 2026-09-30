WITH joined AS (
    SELECT e.*, p.brand
    FROM order_items e
    LEFT JOIN products p
        ON e.product_id = p.id
),
matched_keys AS (
    SELECT order_id
    FROM joined
    EXCEPT ALL
    SELECT order_id
    FROM order_items
    WHERE quantity >= 6
),
verified AS (
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.order_id = k.order_id
),
valid AS (
    SELECT
        *,
        CEIL(quantity) AS quantity_whole,
        COALESCE(brand, 'OTHER') AS brand_filled
    FROM verified
    WHERE brand IS NOT NULL
),
filtered AS (
    SELECT
        *,
        CASE WHEN quantity_whole >= 64 THEN 'High' WHEN quantity_whole >= 14 THEN 'Medium' ELSE 'Low' END AS quantity_whole_tier,
        LOWER(brand) AS brand_lower,
        COALESCE(brand, 'UNKNOWN') AS brand_clean
    FROM valid
    WHERE product_id IS NOT NULL
      AND product_id = 2
),
matched_keys_2 AS (
    SELECT id
    FROM filtered
    INTERSECT ALL
    SELECT id
    FROM order_items
    WHERE order_id = 7
),
verified_order_items AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys_2 k ON s.id = k.id
)
SELECT
    s.order_id,
    s.product_id,
    s.quantity,
    s.unit_price,
    s.quantity_whole,
    s.brand_filled,
    s.brand_lower,
    s.brand_clean,
    (SELECT MAX(s2.revenue) FROM store_sales s2 WHERE s2.product_id = s.product_id) AS max_store_sale_revenue
FROM verified_order_items s
ORDER BY product_id ASC;
