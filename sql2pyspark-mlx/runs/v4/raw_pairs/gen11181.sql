SELECT category, brand, COUNT(CASE WHEN brand = 'Globex' THEN 1 END) AS globex_count
FROM products
GROUP BY category, brand
HAVING COUNT(CASE WHEN brand = 'Globex' THEN 1 END) < 4
ORDER BY brand, globex_count DESC
