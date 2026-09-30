select s.carrier, o.shipping_fee, o.store_id, o.customer_id from shipments s left join orders o on s.order_id = o.id where o.amount between 640 and 1260 and s.carrier in ('UPS', 'FedEx', 'Flash')
