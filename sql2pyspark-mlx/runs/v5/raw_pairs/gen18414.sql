select cost, 'shipment' source from shipments where cost between 28 and 94 union all select shipping_fee, 'order' source from orders order by source;
