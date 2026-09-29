with lines as (
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        value
    from carts
    lateral view explode(attributes) e as k, value
),
flattened as (
    select
        id,
        customer_id,
        created_at,
        k,
        explode_outer(coupon_codes) as code
    from lines
),
filtered as (
    select
        id,
        customer_id,
        created_at,
        k,
        code,
        lower(code) as code_lower,
        coalesce(code, 'OTHER') as code_clean
    from flattened
    where created_at >= to_date('2025-03-01')
),
with_dims as (
    select e.*, c.age
    from filtered e
    inner join customers c
        on e.customer_id <=> c.id
),
metrics as (
    select *, count(age) over (partition by customer_id order by cast(created_at as bigint) asc range between 7200 preceding and current row) as age_count_2h
    from with_dims
    where created_at >= '2023-07-01'
),
scored as (
    select
        id,
        k,
        code,
        code_lower,
        age,
        age_count_2h,
        max(age) over (order by cast(created_at as bigint) asc range between 7200 preceding and current row) as age_max_2h,
        dense_rank() over (partition by code order by age_count_2h asc nulls last) as age_count_2h_dense_rank
    from metrics
)
select *
from (
    select
        k,
        code,
        age_count_2h_dense_rank
    from scored
) src
pivot (
    sum(age_count_2h_dense_rank) as total,
    count(1) as cnt
    for code in ('BUNDLE', 'VIP')
);
