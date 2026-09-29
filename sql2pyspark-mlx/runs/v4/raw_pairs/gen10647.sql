SELECT o.amount, p.method FROM orders o LEFT JOIN payments p ON o.id = p.order_id WHERE p.order_id IS NULL AND o.channel = 'store'
