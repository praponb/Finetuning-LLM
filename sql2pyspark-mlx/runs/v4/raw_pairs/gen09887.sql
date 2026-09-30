SELECT
    i.id,
    i.store_id,
    i.last_restock,
    ss.store_id AS store_sale_store_id,
    ss.revenue
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.revenue BETWEEN 1300 AND 1800 OR ss.sale_date BETWEEN '2025-03-11' AND '2025-08-01'
ORDER BY ss.store_id ASC;
