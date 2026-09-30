SELECT
    id,
    brand,
    category,
    launch_date,
    COALESCE(cost, 0) AS cost_filled
FROM products
WHERE LENGTH(product_name) > 8 AND MONTH(launch_date) IN (1, 5, 8)
