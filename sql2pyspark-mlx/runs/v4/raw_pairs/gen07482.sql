SELECT store_id, product_id, MAX(revenue) - MIN(revenue) AS revenue_range
FROM store_sales
WHERE MONTH(sale_date) IN (7, 9, 11) AND sale_date >= '2023-09-29'
GROUP BY 1, 2
