WITH supplier_summary AS (
    SELECT supplier_id, MAX(price) AS max_price, MAX(stock_qty) AS max_stock_qty
    FROM products
    WHERE category = 'Electronics'
    GROUP BY supplier_id
)
SELECT s.rating, ss.max_price, ss.max_stock_qty
FROM suppliers s
LEFT OUTER JOIN supplier_summary ss ON s.id = ss.supplier_id
WHERE ss.max_price >= 540
