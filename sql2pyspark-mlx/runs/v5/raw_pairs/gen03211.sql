SELECT *
FROM products
WHERE launch_date BETWEEN '2023-08-01' AND '2025-02-20' AND product_name LIKE '%Water%'
ORDER BY id DESC
