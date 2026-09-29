SELECT id, sale_date, DATE_FORMAT(sale_date, 'MMM yyyy') AS sale_date_label FROM store_sales WHERE store_id IN (3, 5) AND units <= 23;
