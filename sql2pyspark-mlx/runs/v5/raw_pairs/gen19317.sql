SELECT last_restock, 'inventory' AS source FROM inventory UNION ALL SELECT delivered_date, 'shipment' AS source FROM shipments WHERE delivered_date <= '2024-08-10';
