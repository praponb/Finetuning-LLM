WITH order_summary AS (
    SELECT order_id, MIN(unit_price) AS min_unit_price
    FROM order_items
    GROUP BY order_id
)
SELECT
    o.id,
    o.channel,
    o.amount,
    os.min_unit_price
FROM orders o
LEFT OUTER JOIN order_summary os ON o.id = os.order_id
ORDER BY id DESC
