SELECT
    i.id,
    i.store_id,
    i.quantity,
    ss.product_id,
    ss.store_id AS store_sale_store_id,
    ss.revenue,
    CASE WHEN ss.revenue >= 4400 THEN 'High' WHEN ss.revenue >= 500 THEN 'Medium' ELSE 'Low' END AS revenue_level
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY product_id DESC, revenue_level
