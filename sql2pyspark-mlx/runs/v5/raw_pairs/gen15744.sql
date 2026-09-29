select
    c.id,
    c.signup_date,
    t.expired_count,
    t.enterprise_count
from customers c
join (select customer_id, count(case when status = 'EXPIRED' then 1 end) as expired_count, count(case when plan = 'enterprise' then 1 end) as enterprise_count
    from subscriptions
    group by customer_id) t on c.id = t.customer_id
where t.enterprise_count >= 7;
