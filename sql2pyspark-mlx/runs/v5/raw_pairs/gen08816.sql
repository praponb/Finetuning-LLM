select channel, max(order_date) as latest_order_date from orders group by 1 order by channel;
