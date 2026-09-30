SELECT SUM(revenue) AS total_revenue, MAX(units) - MIN(units) AS units_range FROM store_sales WHERE units > 18 ORDER BY units_range
