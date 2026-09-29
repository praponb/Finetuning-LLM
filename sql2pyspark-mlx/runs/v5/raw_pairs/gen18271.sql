SELECT MAX(units) AS max_units FROM store_sales WHERE sale_date BETWEEN '2023-04-01' AND '2023-07-01' ORDER BY max_units ASC
