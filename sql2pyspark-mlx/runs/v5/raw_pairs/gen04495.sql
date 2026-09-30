SELECT id, store_id, units
FROM store_sales
WHERE sale_date >= '2023-06-01' AND (revenue IS NULL OR sale_date < '2023-12-01')
