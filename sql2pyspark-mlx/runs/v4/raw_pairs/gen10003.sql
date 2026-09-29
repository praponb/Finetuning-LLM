SELECT
    i.id,
    i.store_id,
    ss.revenue,
    ss.units,
    ss.sale_date
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.store_id IN (1, 2, 5) AND i.quantity >= 200
ORDER BY units DESC, i.store_id DESC;
