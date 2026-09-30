select launch_date, 'product' as source from products union all select shipped_date as launch_date, 'shipment' as source from shipments where order_id is null;
