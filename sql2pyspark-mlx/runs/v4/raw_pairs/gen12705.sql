SELECT a.*, p.discount, p.amount FROM shipments a LEFT JOIN orders p ON a.order_id = p.id
