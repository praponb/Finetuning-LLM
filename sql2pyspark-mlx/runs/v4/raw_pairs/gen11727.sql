SELECT *, revenue + units AS revenue_per_unit, ROUND(units * 1.1, 2) AS units_scaled FROM store_sales WHERE revenue < 3900 ORDER BY units_scaled DESC, id LIMIT 50
