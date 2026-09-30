SELECT store_id, MAX(revenue) AS max_revenue
FROM store_sales
WHERE sale_date > '2023-05-31'
GROUP BY store_id
