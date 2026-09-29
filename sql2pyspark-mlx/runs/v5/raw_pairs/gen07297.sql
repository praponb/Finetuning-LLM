SELECT
    id,
    price,
    cost,
    stock_qty,
    COALESCE(price, 0) AS price_filled,
    CONCAT(product_name, ' - ', brand) AS product_name_brand
FROM products
WHERE ARRAY_CONTAINS(tags, 'bestseller');
