SELECT s.cost, s.delivered_date, s.status, o.discount, o.order_date FROM shipments s INNER JOIN orders o ON o.id = s.order_id WHERE o.discount <= 0.26;
