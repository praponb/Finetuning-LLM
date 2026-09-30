SELECT o.status, MIN(s.delivered_date) AS first_delivered_date, MIN(s.shipped_date) AS first_shipped_date
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
WHERE o.order_date <= '2024-06-01'
GROUP BY o.status;
