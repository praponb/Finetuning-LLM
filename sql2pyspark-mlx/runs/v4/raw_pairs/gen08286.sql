SELECT ss.units, p.category FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id WHERE p.stock_qty <= 213 ORDER BY category;
