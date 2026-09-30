SELECT p.supplier_id, COUNT(ss.revenue) AS revenue_count, AVG(ss.units) AS avg_units FROM store_sales ss RIGHT JOIN products p ON ss.product_id = p.id WHERE p.price <= 300 GROUP BY p.supplier_id
