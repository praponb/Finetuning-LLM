SELECT sale_date, store_id
FROM store_sales
WHERE sale_date > '2025-01-01'
UNION
SELECT sale_date, store_id
FROM store_sales
WHERE revenue < 1900
ORDER BY sale_date DESC;
