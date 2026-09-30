SELECT id, sqft
FROM stores
WHERE opened_date < '2023-12-01' AND store_name LIKE 'Si%'
ORDER BY id DESC
LIMIT 10
