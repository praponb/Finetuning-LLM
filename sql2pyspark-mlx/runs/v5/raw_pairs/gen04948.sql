with with_last as (
    select
        s.id,
        s.country,
        s.signup_date,
        s.is_active,
        s.age,
        x.order_date
    from customers s
    join lateral (
            select c.order_date
            from orders c
            where c.customer_id = s.id
              and c.order_date <= s.signup_date
            order by c.order_date desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    country,
    signup_date,
    is_active,
    age,
    order_date
from with_last;
