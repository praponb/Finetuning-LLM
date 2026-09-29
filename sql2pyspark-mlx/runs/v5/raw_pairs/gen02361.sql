select shipments.id, shipments.cost, shipments.shipped_date, shipments.delivered_date from shipments where shipments.order_id in (select id from orders where shipping_fee between 10 and 38)
