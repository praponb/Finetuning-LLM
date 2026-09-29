select
    plan,
    sum(case when status = 'CANCELLED' then 1 else 0 end) as cancelled_count,
    concat_ws(', ', sort_array(collect_set(status))) as status_values,
    count(case when status = 'ACTIVE' then 1 end) as active_count
from subscriptions
where plan not in ('basic', 'standard', 'enterprise') and customer_id in (6, 16, 28, 33) and plan = 'basic'
group by plan
having sum(case when status = 'CANCELLED' then 1 else 0 end) > 71;
