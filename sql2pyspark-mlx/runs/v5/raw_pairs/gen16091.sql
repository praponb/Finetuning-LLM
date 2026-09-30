WITH order_items_by_product AS (
    SELECT product_id, SUM(quantity * unit_price) AS revenue, COUNT(*) AS cnt
    FROM order_items
    WHERE unit_price >= 300
    GROUP BY product_id
)
SELECT
    p.id,
    p.launch_date,
    p.brand,
    oibp.revenue,
    oibp.cnt
FROM products p
INNER JOIN order_items_by_product oibp ON p.id = oibp.product_id
WHERE oibp.revenue > 42
