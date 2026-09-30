WITH OrderTotals AS (
    SELECT order_id, SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END) AS cash_count
    FROM payments
    GROUP BY order_id
)
SELECT o.id, o.shipping_fee, O.cash_count
FROM orders o
INNER JOIN OrderTotals O ON o.id = O.order_id
ORDER BY cash_count;
