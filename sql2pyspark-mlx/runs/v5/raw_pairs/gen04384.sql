SELECT *, FLOOR(revenue) AS revenue_whole
FROM store_sales
WHERE revenue BETWEEN 1700 AND 2900 AND store_id IN (1, 4, 5) AND units BETWEEN 31 AND 35
