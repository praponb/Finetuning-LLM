select
    channel,
    avg(shipping_fee) as avg_shipping_fee,
    count(case when channel = 'partner' then 1 end) as partner_count,
    max(amount) as max_amount
from orders
group by 1
order by max_amount desc
