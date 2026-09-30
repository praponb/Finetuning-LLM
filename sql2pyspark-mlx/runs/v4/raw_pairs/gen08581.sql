select shipments.id, shipments.status, shipments.carrier from shipments where shipments.order_id in (select id from orders where amount between 870 and 1320)
