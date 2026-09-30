SELECT
    DATE_TRUNC('MONTH', c.signup_date) AS signup_month,
    c.segment,
    COUNT(DISTINCT s.cost) AS unique_orders,
    MAX(s.delivered_date) AS latest_delivered_date
FROM shipments s
JOIN orders o ON o.id = s.order_id
JOIN customers c ON o.customer_id = c.id
WHERE o.order_date BETWEEN '2024-02-01' AND '2024-11-07'
GROUP BY DATE_TRUNC('MONTH', c.signup_date), c.segment;
