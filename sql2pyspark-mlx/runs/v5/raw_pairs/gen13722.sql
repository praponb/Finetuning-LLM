SELECT
    id,
    stock_qty,
    brand,
    SUBSTRING(product_name, 1, 2) product_name_prefix
FROM products
WHERE (ARRAY_CONTAINS(tags, 'eco') AND category = 'Electronics') OR supplier_id = 38;
