SELECT *, FLOOR(revenue) AS revenue_whole
FROM store_sales
WHERE revenue IS NOT NULL AND store_id IN (3, 4)
