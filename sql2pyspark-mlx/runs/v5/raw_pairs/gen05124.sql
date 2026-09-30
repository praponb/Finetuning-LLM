select o.id, o.status, count(s.id) shipment_count, count(distinct s.cost) unique_carriers from orders o left join shipments s on o.id = s.order_id group by o.id, o.status;
