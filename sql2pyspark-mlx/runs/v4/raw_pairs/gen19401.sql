SELECT id, sale_date, units, LEAD(units, 1) OVER (ORDER BY sale_date, id) AS next_units FROM store_sales
