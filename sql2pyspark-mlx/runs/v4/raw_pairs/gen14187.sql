with base as (
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        substring(plan, 1, 3) as plan_prefix,
        length(status) as status_length
    from subscriptions
    where end_date > date '2024-03-03'
      and customer_id is not null
)
select s.*, b.country
from base s
left join customers b
    on s.customer_id = b.id;
