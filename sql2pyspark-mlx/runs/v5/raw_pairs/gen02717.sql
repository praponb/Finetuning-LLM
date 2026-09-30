select status, customer_id, discount + amount as discount_plus_amount, substring(channel, 1, 3) as channel_prefix from orders where discount <= 0.36 or status not in ('RETURNED', 'SHIPPED')
