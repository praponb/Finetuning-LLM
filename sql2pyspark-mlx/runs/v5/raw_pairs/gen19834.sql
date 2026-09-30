SELECT s.id, s.sqft, s.opened_date, s.store_name FROM stores s WHERE s.id IN (SELECT store_id FROM store_sales WHERE sale_date BETWEEN '2023-08-27' AND '2024-08-06')
