select p.id, p.status, p.paid_at, p.method, o.store_id, o.channel, o.status as order_status from payments p join orders o on p.order_id = o.id order by store_id desc
