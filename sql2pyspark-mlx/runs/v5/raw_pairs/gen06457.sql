with sessions_by_customer as (
    select customer_id, max(started_at) as last_started_at, min(started_at) as first_started_at
    from sessions
    where country != 'DE'
    group by customer_id
)
select c.signup_date, sbc.last_started_at, sbc.first_started_at
from customers c
left join sessions_by_customer sbc on c.id = sbc.customer_id;
