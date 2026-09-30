SELECT id, store_id, units, revenue, sale_date, AVG(units) OVER (PARTITION BY store_id) AS store_id_avg_units, FIRST_VALUE(sale_date) OVER (ORDER BY revenue, id) AS first_sale_date FROM store_sales
