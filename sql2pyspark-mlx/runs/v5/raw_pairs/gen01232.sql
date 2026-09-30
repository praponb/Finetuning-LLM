select 'product' as source, price from products union select 'shipment' as source, cost from shipments where order_id is not null order by price;
