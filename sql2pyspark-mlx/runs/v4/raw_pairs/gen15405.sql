select id, channel, order_date from orders where status = 'PENDING' intersect select id, channel, order_date from orders where channel in ('partner', 'web', 'mobile')
