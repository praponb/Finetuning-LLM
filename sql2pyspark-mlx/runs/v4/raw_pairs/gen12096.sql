SELECT MAX(revenue) - MIN(revenue) AS revenue_range
FROM store_sales
ORDER BY revenue_range;
