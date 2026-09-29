WITH inventory_by_product AS (
    SELECT product_id, COUNT(quantity) AS quantity_count
    FROM inventory
    WHERE product_id IN (2, 9, 10)
    GROUP BY product_id
)
SELECT
    p.id,
    p.brand,
    p.stock_qty,
    ibp.quantity_count
FROM products p
INNER JOIN inventory_by_product ibp ON p.id = ibp.product_id
