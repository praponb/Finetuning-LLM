select store_id, units, revenue, year(sale_date) as sale_date_year from store_sales where sale_date < '2024-03-01' and store_id = 6
