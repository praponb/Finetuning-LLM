select
    channel,
    min(order_date) first_order_date,
    round(stddev(amount), 2) stddev_amount,
    sort_array(collect_set(channel)) channel_list
from orders
group by channel
order by channel desc, first_order_date
