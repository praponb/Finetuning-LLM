select id, discount, length(channel) as channel_length from orders where status = 'CANCELLED' and customer_id in (7, 14) and status in ('PENDING', 'CANCELLED', 'RETURNED');
