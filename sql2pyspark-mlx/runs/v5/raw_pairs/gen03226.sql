SELECT
    p.id,
    p.supplier_id,
    p.stock_qty,
    t.stddev_unit_price
FROM products p
LEFT JOIN (SELECT product_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
    FROM order_items
    WHERE order_id IN (18, 21, 33)
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY supplier_id DESC;
