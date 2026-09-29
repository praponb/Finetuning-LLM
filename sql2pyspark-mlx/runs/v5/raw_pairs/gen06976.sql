WITH StoreSalesByProduct AS (
    SELECT product_id, COUNT(units) AS units_count
    FROM store_sales
    GROUP BY product_id
)
SELECT
    p.id,
    p.price,
    p.product_name,
    S.units_count
FROM products p
LEFT JOIN StoreSalesByProduct S ON p.id = S.product_id
