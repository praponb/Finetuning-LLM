SELECT 'transaction' AS source, amount FROM transactions UNION SELECT 'shipment' AS source, cost FROM shipments WHERE status = 'RETURNED' ORDER BY amount
