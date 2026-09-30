select s.carrier, s.delivered_date, o.amount from shipments s right outer join orders o on o.id = s.order_id
