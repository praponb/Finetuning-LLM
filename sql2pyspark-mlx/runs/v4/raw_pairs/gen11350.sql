SELECT DATE_FORMAT(last_restock, 'yyyy-MM') last_restock_month, SUM(quantity) total_quantity
FROM inventory
WHERE YEAR(last_restock) >= 2023 AND last_restock <= '2024-03-01'
GROUP BY DATE_FORMAT(last_restock, 'yyyy-MM')
