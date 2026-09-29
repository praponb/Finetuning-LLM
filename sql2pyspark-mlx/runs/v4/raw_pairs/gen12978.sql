select o.store_id, o.channel, o.status, s.status as shipment_status, s.shipped_date from orders o left join shipments s on o.id = s.order_id order by s.shipped_date asc;
