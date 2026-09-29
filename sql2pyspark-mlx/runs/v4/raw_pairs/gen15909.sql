SELECT
    i.last_restock,
    i.quantity,
    ss.units,
    ss.revenue,
    ROUND(i.quantity, 1) AS quantity_rounded
FROM inventory i
JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id = 5;
