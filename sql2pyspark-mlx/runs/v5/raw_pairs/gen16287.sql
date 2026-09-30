SELECT tags, brand, launch_date
FROM products
WHERE brand = 'Acme' AND price > 360
ORDER BY launch_date
