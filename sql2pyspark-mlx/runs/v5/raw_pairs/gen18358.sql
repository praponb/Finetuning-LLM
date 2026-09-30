SELECT p.category, p.cost, ss.units, ss.revenue, ss.store_id FROM products p LEFT JOIN store_sales ss ON ss.product_id = p.id WHERE ARRAY_CONTAINS(p.tags, 'premium') AND ss.product_id IN (3, 10)
