select s.cost, o.store_id, o.shipping_fee, o.order_date from shipments s join orders o on s.order_id = o.id where o.customer_id in (30, 35, 39) and s.carrier = 'FedEx';
