with cleaned as (
    select
        id,
        customer_id,
        store_id,
        amount,
        discount,
        shipping_fee,
        amount * discount as amount_times_discount,
        coalesce(channel, 'OTHER') as channel_clean
    from orders
    where store_id is not null
),
joined as (
    select
        s.*,
        d.signup_date,
        d.age,
        d.name
    from cleaned s
    inner join customers d
        on s.customer_id = d.id
),
latest as (
    select *
    from joined
    qualify row_number() over (partition by store_id, customer_id order by age desc nulls first, id asc) = 1
),
eligible as (
    select s.*
    from latest s
    where exists (
            select 1
            from order_items c
            where c.order_id = s.id
              and c.unit_price <= 320
        )
)
select
    s.id,
    s.customer_id,
    s.signup_date,
    s.age,
    (select max(t2.balance) from accounts t2 where t2.customer_id = s.customer_id) as max_account_balance
from eligible s
order by customer_id desc nulls first;
