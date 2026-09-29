SELECT p.category, MIN(ss.sale_date) AS min_sale_date, SUM(ss.units) AS total_units FROM store_sales ss JOIN products p ON ss.product_id = p.id WHERE ss.revenue IS NULL GROUP BY p.category;
