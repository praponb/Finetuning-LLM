SELECT sale_date FROM store_sales WHERE store_id IN (2, 3, 4, 6) UNION SELECT sale_date FROM store_sales WHERE sale_date <= '2024-10-01';
