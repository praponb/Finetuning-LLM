SELECT cost FROM products WHERE category <> 'Grocery' EXCEPT SELECT revenue FROM store_sales WHERE store_id IN (1, 2, 5, 6) ORDER BY cost
