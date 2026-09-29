SELECT store_id, ROUND(AVG(revenue), 2) AS avg_revenue
FROM store_sales
WHERE sale_date BETWEEN '2023-07-01' AND '2024-04-01'
GROUP BY store_id;
