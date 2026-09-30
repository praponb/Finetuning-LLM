WITH ProductStats AS (
    SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range, SUM(unit_price * quantity) AS revenue
    FROM order_items
    GROUP BY product_id
)
SELECT p.supplier_id, P.quantity_range, P.revenue
FROM products p
LEFT OUTER JOIN ProductStats P ON p.id = P.product_id
WHERE P.quantity_range > 57
