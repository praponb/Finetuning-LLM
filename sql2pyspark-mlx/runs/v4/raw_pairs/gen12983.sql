SELECT product_id, store_id, MAX(sale_date) AS last_sale_date
FROM store_sales
WHERE units < 25 AND units <> 29
GROUP BY product_id, store_id
