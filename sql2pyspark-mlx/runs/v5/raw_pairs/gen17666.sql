SELECT
    i.id,
    i.store_id,
    i.product_id,
    i.quantity
FROM inventory i
WHERE i.product_id IN (SELECT id FROM products WHERE YEAR(launch_date) >= 2025)
