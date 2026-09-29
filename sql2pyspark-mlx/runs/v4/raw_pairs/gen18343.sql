SELECT city, 'supplier' AS source
FROM suppliers
WHERE country IN ('DE', 'TH', 'UK')
UNION ALL
SELECT city, 'store' AS source
FROM stores
WHERE store_name IS NOT NULL
