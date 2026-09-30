with subscriptions_by_customer as (
    select customer_id, concat_ws(', ', sort_array(collect_set(status))) as status_values, count(monthly_fee) as monthly_fee_count
    from subscriptions
    where status = 'PAUSED'
    group by customer_id
)
select
    c.id,
    c.country,
    sbc.status_values,
    sbc.monthly_fee_count
from customers c
join subscriptions_by_customer sbc on c.id = sbc.customer_id
order by country asc;
