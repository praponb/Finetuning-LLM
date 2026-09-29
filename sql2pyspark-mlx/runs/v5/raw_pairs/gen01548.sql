SELECT p.stock_qty, t.units_count, t.unique_stores
FROM products p
INNER JOIN (SELECT product_id, COUNT(units) AS units_count, COUNT(DISTINCT units) AS unique_stores
    FROM store_sales
    WHERE product_id IN (1, 7, 9)
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY p.stock_qty;
