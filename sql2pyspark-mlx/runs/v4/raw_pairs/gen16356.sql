SELECT ss.id, ss.units, p.category FROM store_sales ss LEFT OUTER JOIN products p ON p.id = ss.product_id ORDER BY ss.id DESC LIMIT 50;
