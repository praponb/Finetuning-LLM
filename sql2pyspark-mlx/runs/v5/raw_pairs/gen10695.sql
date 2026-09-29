SELECT
    id,
    tags,
    brand,
    category,
    INITCAP(TRIM(category)) AS category_title
FROM products
WHERE LENGTH(product_name) > 13;
