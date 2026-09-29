SELECT shipments.id, shipments.status FROM shipments WHERE shipments.carrier IN ('FedEx', 'Flash', 'UPS') AND shipments.order_id IN (SELECT id FROM orders WHERE channel = 'web')
