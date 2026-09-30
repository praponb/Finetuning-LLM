SELECT
    i.id,
    i.product_id,
    i.quantity,
    ss.revenue,
    ss.store_id,
    ss.product_id AS store_sale_product_id
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
ORDER BY revenue, store_sale_product_id DESC;
