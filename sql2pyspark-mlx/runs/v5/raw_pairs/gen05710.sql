select p.id, p.status, p.method, o.discount, trim(o.channel) clean_channel from payments p left join orders o on p.order_id = o.id where o.shipping_fee <= 14
