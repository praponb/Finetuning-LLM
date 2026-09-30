SELECT
    brand,
    launch_date,
    tags,
    SUBSTRING(product_name, 1, 3) AS product_name_prefix
FROM products;
