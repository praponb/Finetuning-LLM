SELECT ss.id, ss.product_id, ss.revenue, ss.units FROM store_sales ss WHERE ss.product_id NOT IN (SELECT id FROM products WHERE ARRAY_CONTAINS(tags, 'premium'))
