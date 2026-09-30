WITH product_totals AS (
    SELECT product_id, ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM order_items
    WHERE order_id = 16
    GROUP BY product_id
)
SELECT p.supplier_id, p.brand, pt.avg_unit_price
FROM products p
LEFT OUTER JOIN product_totals pt ON p.id = pt.product_id
ORDER BY supplier_id DESC;
