SELECT
    id,
    launch_date,
    category,
    cost
FROM products
WHERE product_name NOT LIKE 'W%'
ORDER BY cost DESC
