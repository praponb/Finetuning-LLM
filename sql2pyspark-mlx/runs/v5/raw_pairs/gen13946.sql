select
    s.id,
    s.delivered_date,
    o.amount,
    o.shipping_fee,
    o.channel,
    upper(o.channel) as channel_upper
from shipments s
join orders o on o.id = s.order_id
where o.amount <= 520 or s.cost <= 59;
