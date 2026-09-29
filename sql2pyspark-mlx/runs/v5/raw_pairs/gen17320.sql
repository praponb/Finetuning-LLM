SELECT *, LEAST(revenue, units) AS least_revenue_units, COALESCE(units, 0) AS units_filled FROM store_sales WHERE revenue IS NOT NULL AND MONTH(sale_date) IN (1, 5, 8) AND store_id IN (1, 4)
