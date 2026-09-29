WITH base AS (
    SELECT *, LEAST(unit_price, quantity) AS least_unit_price_quantity
    FROM order_items
    WHERE (quantity BETWEEN 5 AND 6 AND quantity > 5) OR unit_price > 230
),
agg AS (
    SELECT product_id, order_id, MIN(quantity) AS min_quantity
    FROM base
    GROUP BY product_id, order_id
)
SELECT product_id, min_quantity
FROM agg
WHERE min_quantity >= 3.4
ORDER BY min_quantity DESC
