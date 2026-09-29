SELECT amount, 'transaction' AS kind FROM transactions UNION SELECT cost, 'shipment' AS kind FROM shipments WHERE status = 'DELIVERED'
