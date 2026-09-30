WITH OrderStats AS (
    SELECT order_id, COUNT(CASE WHEN method = 'card' THEN 1 END) AS card_count
    FROM payments
    WHERE amount <= 1180
    GROUP BY order_id
)
SELECT o.customer_id, o.channel, O.card_count
FROM orders o
LEFT OUTER JOIN OrderStats O ON o.id = O.order_id
