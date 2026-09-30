SELECT carrier, order_id FROM shipments EXCEPT DISTINCT SELECT carrier, order_id FROM shipments WHERE carrier <> 'UPS';
