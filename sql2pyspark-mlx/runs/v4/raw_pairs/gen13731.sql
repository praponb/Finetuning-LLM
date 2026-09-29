SELECT store_id, product_id FROM store_sales EXCEPT DISTINCT SELECT store_id, product_id FROM store_sales WHERE sale_date BETWEEN '2023-06-21' AND '2024-06-01'
