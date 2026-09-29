SELECT id, units, product_id FROM store_sales WHERE revenue BETWEEN 2100 AND 3300 EXCEPT SELECT id, units, product_id FROM store_sales WHERE MONTH(sale_date) IN (2, 3, 6)
