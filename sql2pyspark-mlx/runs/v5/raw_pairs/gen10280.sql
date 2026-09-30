SELECT id, units, revenue
FROM store_sales
WHERE sale_date <= '2024-01-01' OR sale_date >= '2024-12-01'
ORDER BY revenue
