SELECT store_id, MAX(sale_date) AS latest_sale_date FROM store_sales WHERE store_id = 5 GROUP BY store_id ORDER BY latest_sale_date DESC;
