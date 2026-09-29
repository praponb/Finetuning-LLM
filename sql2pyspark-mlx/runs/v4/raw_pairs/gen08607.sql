SELECT
    id,
    cost,
    product_name,
    ROUND(stock_qty / price, 2) AS stock_qty_per_price,
    ARRAY_JOIN(tags, '|') AS tags_text
FROM products
WHERE price IS NOT NULL AND product_name NOT LIKE '%Chair%'
ORDER BY stock_qty_per_price DESC, id
LIMIT 5;
