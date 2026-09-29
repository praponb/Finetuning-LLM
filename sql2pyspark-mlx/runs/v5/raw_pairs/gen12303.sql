with exploded as (
    select
        id,
        customer_id,
        attr_key,
        v
    from carts
    lateral view explode(attributes) li as attr_key, v
),
filtered as (
    select
        *,
        length(attr_key) as attr_key_length,
        lower(attr_key) as attr_key_lower
    from exploded
    where attr_key is not null
      and attr_key is distinct from 'device'
),
joined as (
    select
        src.*,
        b.age,
        b.email,
        b.credit_limit
    from filtered src
    left join customers b
        on src.customer_id <=> b.id
),
with_dims as (
    select e.*, b.country
    from joined e
    left join customers b
        on e.customer_id = b.id
    where b.segment != 'Corporate'
),
with_dims_carts as (
    select
        a.*,
        c.segment,
        c.credit_limit as customer_credit_limit
    from with_dims a
    left join customers c
        on a.customer_id = c.id
)
select
    d.city,
    src.attr_key,
    count(*) as cnt,
    sum(src.attr_key_length) as total_attr_key_length,
    grouping_id() as gid
from with_dims_carts src
inner join customers d on src.customer_id = d.id
group by grouping sets ((d.city, src.attr_key), (d.city), ())
order by attr_key nulls last;
