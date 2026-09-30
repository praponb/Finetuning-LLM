SELECT product_id, MIN(sale_date) AS min_sale_date, SUM(revenue) AS total_revenue
FROM store_sales
WHERE product_id IN (2, 7, 10) AND sale_date BETWEEN '2023-03-19' AND '2023-07-01'
GROUP BY product_id
HAVING SUM(revenue) < 13000
