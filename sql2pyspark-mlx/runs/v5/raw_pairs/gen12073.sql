SELECT ss.id, ss.store_id
FROM store_sales ss
WHERE ss.units BETWEEN 20 AND 32 AND ss.product_id NOT IN (SELECT id FROM products WHERE stock_qty < 381)
ORDER BY ss.id DESC
