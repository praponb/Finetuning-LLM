SELECT
    i.product_id,
    i.quantity,
    ss.units,
    ss.sale_date
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE i.product_id IN (1, 5, 8, 9);
