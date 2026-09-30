SELECT
    id,
    supplier_id,
    price,
    launch_date,
    FIRST_VALUE(launch_date) OVER (PARTITION BY supplier_id ORDER BY price DESC, id) AS first_launch_date
FROM products
WHERE brand <> 'Globex'
ORDER BY price, id DESC
