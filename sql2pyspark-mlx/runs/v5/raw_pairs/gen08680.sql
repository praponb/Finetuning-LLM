SELECT sale_date, product_id FROM store_sales WHERE units IS NULL EXCEPT SELECT sale_date, product_id FROM store_sales WHERE units BETWEEN 28 AND 33;
