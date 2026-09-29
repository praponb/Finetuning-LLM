SELECT
    product_name,
    launch_date,
    brand,
    FLOOR(cost) AS cost_whole,
    REGEXP_REPLACE(product_name, ' ', '') AS product_name_slug
FROM products
WHERE LOWER(product_name) LIKE '%unn%';
