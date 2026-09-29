select channel, count(case when channel = 'partner' then 1 end) as partner_count, max(order_date) as latest_order_date from orders where discount < 0.19 group by channel order by partner_count desc
