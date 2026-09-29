select s.id, s.status, s.carrier, o.order_date, o.channel, o.shipping_fee from shipments s left join orders o on s.order_id = o.id
