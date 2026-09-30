SELECT sale_date, product_id FROM store_sales WHERE revenue BETWEEN 2100 AND 3700 INTERSECT SELECT sale_date, product_id FROM store_sales WHERE revenue < 2300 ORDER BY product_id
