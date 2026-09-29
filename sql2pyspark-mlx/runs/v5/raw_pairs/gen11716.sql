SELECT shipped_date FROM shipments WHERE delivered_date BETWEEN '2023-04-01' AND '2025-09-01' EXCEPT SELECT sale_date FROM store_sales WHERE product_id IN (1, 2, 8) ORDER BY shipped_date DESC;
