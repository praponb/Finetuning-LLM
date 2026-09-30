SELECT s.*, p.channel FROM shipments s LEFT JOIN orders p ON s.order_id = p.id;
