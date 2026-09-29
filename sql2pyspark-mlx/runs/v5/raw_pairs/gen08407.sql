select s.id, s.shipped_date, s.delivered_date, o.channel, o.discount, o.order_date from shipments s join orders o on s.order_id = o.id where s.carrier in ('DHL', 'UPS', 'FedEx')
