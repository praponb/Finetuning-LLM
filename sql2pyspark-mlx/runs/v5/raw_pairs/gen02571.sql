SELECT
    supplier_id,
    product_name,
    price,
    SUBSTRING(product_name, 1, 3) AS product_name_prefix
FROM products
WHERE supplier_id IS NOT NULL AND cost BETWEEN 340 AND 390;
