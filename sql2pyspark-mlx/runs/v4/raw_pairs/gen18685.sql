SELECT id, store_id, units, sale_date, FIRST_VALUE(sale_date) OVER (PARTITION BY store_id ORDER BY units, id) AS first_sale_date FROM store_sales WHERE sale_date > '2023-08-01'
