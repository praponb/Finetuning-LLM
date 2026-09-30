with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        explode(coupon_codes) as code
    from carts
),
base as (
    -- Keep recent, valid rows
    select
        *,
        upper(code) as code_upper,
        coalesce(code, 'N/A') as code_clean
    from exploded
    where customer_id = 31
),
with_dims as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.code,
        a.code_upper,
        a.code_clean,
        c.name
    from base a
    left join customers c
        on a.customer_id = c.id
    where c.is_active
),
joined as (
    -- Attach customers attributes
    select
        src.*,
        c.country,
        c.email,
        c.age
    from with_dims src
    left join customers c
        on src.customer_id = c.id
),
filtered as (
    -- Keep recent, valid rows
    select *, coalesce(code, 'Unknown') as code_filled
    from joined
    where code is not null
)
-- Aggregate with conditional metrics
select
    customer_id,
    min(age) as min_age,
    sum(age) filter (where code = 'WELCOME5') as welcome5_total_age
from filtered
group by 1
order by customer_id;
