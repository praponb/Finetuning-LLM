SELECT s.id, s.delivered_date, s.cost, o.amount, o.discount FROM shipments s LEFT JOIN orders o ON s.order_id = o.id WHERE o.shipping_fee >= 29 AND o.discount IS NOT NULL ORDER BY delivered_date
