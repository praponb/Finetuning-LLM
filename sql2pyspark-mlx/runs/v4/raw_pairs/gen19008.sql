select c.city, t.monthly_fee_range, t.avg_monthly_fee
from customers c
left join (select customer_id, max(monthly_fee) - min(monthly_fee) monthly_fee_range, avg(monthly_fee) avg_monthly_fee
    from subscriptions
    group by customer_id) t on c.id = t.customer_id
order by c.city desc;
