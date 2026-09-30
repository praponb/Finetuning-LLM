select date_trunc('MONTH', s.opened_date) as opened_month, count(o.shipping_fee) as shipping_fee_count, sum(case when o.channel = 'store' then 1 else 0 end) as store_count
from orders o
right join customers c on o.customer_id = c.id
inner join stores s on s.id = o.store_id
group by date_trunc('MONTH', s.opened_date)
order by shipping_fee_count;
