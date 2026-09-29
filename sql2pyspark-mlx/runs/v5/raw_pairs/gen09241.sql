SELECT o.shipping_fee, t.pct_success
FROM orders o
LEFT JOIN (SELECT order_id, ROUND((100 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_success
    FROM payments
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.pct_success > 34
ORDER BY shipping_fee DESC
