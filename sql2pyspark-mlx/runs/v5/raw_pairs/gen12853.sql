SELECT
    id,
    supplier_id,
    launch_date,
    price,
    FIRST_VALUE(price) OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS first_price
FROM products
