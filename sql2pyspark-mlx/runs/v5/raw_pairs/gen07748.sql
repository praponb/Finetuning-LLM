SELECT s.id, s.order_id FROM shipments s WHERE s.shipped_date BETWEEN '2023-08-26' AND '2025-10-11' AND s.order_id NOT IN (SELECT id FROM orders WHERE status = 'RETURNED');
