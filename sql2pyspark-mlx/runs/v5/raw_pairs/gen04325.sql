SELECT
    brand,
    category,
    COUNT(stock_qty) AS stock_qty_count,
    MIN(stock_qty) AS min_stock_qty
FROM products
WHERE category = 'Grocery'
GROUP BY brand, category;
