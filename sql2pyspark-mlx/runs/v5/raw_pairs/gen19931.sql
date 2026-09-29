select
    channel,
    status,
    date_format(order_date, 'yyyy-MM') as order_month,
    avg(shipping_fee) as avg_shipping_fee,
    count(discount) as discount_count,
    round((100 * sum(case when channel = 'mobile' then 1 else 0 end)) / count(*), 1) as pct_mobile
from orders
group by channel, status, date_format(order_date, 'yyyy-MM');
