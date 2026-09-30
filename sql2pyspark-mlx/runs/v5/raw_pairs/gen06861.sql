SELECT s.id, s.delivered_date, s.order_id, s.cost, o.order_date, o.status FROM shipments s INNER JOIN orders o ON o.id = s.order_id WHERE s.cost IS NOT NULL OR s.carrier NOT IN ('Kerry', 'DHL')
