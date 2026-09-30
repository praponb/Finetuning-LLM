WITH order_items_by_product AS (
    SELECT product_id, SUM(quantity * unit_price) AS revenue, AVG(unit_price) AS avg_unit_price
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.product_name,
    oibp.revenue,
    oibp.avg_unit_price
FROM products p
LEFT JOIN order_items_by_product oibp ON p.id = oibp.product_id
ORDER BY p.id, oibp.revenue DESC
