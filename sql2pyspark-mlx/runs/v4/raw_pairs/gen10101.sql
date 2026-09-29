SELECT sale_date
FROM store_sales
UNION
SELECT last_restock
FROM inventory
ORDER BY sale_date;
