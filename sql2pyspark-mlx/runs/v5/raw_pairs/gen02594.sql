SELECT city, store_name
FROM stores
WHERE opened_date BETWEEN '2024-05-01' AND '2024-11-01'
UNION
SELECT city, store_name
FROM stores
WHERE store_name IS NOT NULL
ORDER BY store_name;
