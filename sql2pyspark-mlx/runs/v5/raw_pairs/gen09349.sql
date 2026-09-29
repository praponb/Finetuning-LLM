with CustomerStats as (
    select customer_id, sum(monthly_fee) as total_monthly_fee, max(monthly_fee) as max_monthly_fee
    from subscriptions
    where month(start_date) in (2, 10, 11)
    group by customer_id
)
select c.age, C.total_monthly_fee, C.max_monthly_fee
from customers c
join CustomerStats C on c.id = C.customer_id
where C.max_monthly_fee > 100;
