SELECT s.id, s.cost, o.store_id FROM shipments s JOIN orders o ON s.order_id = o.id WHERE s.status IN ('IN_TRANSIT', 'LOST', 'DELIVERED') ORDER BY s.id DESC
