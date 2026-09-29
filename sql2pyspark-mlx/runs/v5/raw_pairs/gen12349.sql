SELECT 'shipment' AS origin, delivered_date FROM shipments UNION SELECT 'inventory' AS origin, last_restock FROM inventory;
