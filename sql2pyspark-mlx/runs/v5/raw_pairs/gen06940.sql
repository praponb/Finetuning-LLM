with flattened as (
    select
        id,
        customer_id,
        created_at,
        explode(coupon_codes) as element
    from carts
),
joined as (
    select
        e.*,
        p.email,
        p.name,
        p.segment
    from flattened e
    left join customers p
        on e.customer_id <=> p.id
),
cleaned as (
    select *, initcap(trim(name)) as name_title
    from joined
    where element is not distinct from 'BUNDLE'
      and customer_id is not null
),
joined_carts as (
    select
        src.id,
        src.customer_id,
        p.credit_limit,
        p.age,
        p.city
    from cleaned src
    left join customers p
        on src.customer_id <=> p.id
),
with_windows as (
    select
        id,
        customer_id,
        credit_limit,
        stddev_samp(credit_limit) over (partition by city) as city_stddev_samp_credit_limit
    from joined_carts
)
select
    id,
    credit_limit,
    city_stddev_samp_credit_limit
from with_windows
order by id desc nulls last
