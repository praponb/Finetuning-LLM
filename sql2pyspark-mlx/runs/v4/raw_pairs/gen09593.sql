WITH product_totals AS (
    SELECT product_id, MIN(quantity) AS min_quantity
    FROM order_items
    GROUP BY product_id
)
SELECT p.id, p.launch_date, pt.min_quantity
FROM products p
INNER JOIN product_totals pt ON p.id = pt.product_id;
