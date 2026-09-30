SELECT d.region, COUNT(*) AS rows_total, SUM(src.revenue) AS total_revenue, SUM(src.units) AS total_units FROM store_sales src JOIN stores d ON src.store_id = d.id GROUP BY ROLLUP (d.region)
