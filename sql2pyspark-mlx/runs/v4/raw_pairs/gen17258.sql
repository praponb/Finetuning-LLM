select channel, min(discount) as min_discount from orders group by channel order by min_discount desc, channel limit 3
