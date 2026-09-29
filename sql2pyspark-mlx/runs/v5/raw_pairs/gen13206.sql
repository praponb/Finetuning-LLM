SELECT ss.id, ss.product_id, ss.sale_date FROM store_sales ss WHERE ss.units BETWEEN 13 AND 33 AND ss.product_id IN (SELECT id FROM products WHERE ARRAY_CONTAINS(tags, 'premium')) ORDER BY ss.id DESC
