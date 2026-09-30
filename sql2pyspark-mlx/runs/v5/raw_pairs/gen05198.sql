SELECT ss.store_id, ss.revenue, p.cost
FROM store_sales ss
LEFT JOIN products p ON p.id = ss.product_id
WHERE ss.product_id IN (1, 2)
ORDER BY cost DESC, store_id DESC;
