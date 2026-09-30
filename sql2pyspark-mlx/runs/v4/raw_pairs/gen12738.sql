SELECT DATE_FORMAT(o.order_date, 'yyyy-MM') AS order_month, SUM(CASE WHEN s.status = 'LOST' THEN s.cost ELSE 0 END) AS lost_cost, SUM(CASE WHEN s.status = 'IN_TRANSIT' THEN 1 ELSE 0 END) AS in_transit_count
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
GROUP BY DATE_FORMAT(o.order_date, 'yyyy-MM');
