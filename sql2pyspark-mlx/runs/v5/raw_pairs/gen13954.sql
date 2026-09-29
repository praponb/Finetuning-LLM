SELECT 'account' AS kind, status FROM accounts WHERE balance IS NOT NULL UNION SELECT 'shipment' AS kind, status FROM shipments;
