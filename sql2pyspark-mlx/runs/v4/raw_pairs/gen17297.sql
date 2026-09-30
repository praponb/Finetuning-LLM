SELECT id, units
FROM store_sales
WHERE store_id = 5
UNION
SELECT id, units
FROM store_sales
WHERE MONTH(sale_date) IN (4, 10, 12)
ORDER BY id DESC;
