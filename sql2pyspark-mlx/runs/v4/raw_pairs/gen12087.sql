SELECT
    id,
    category,
    product_name,
    CONCAT_WS('-', category, brand) AS category_brand_key
FROM products
WHERE product_name LIKE '%Yog%'
ORDER BY id
LIMIT 3
