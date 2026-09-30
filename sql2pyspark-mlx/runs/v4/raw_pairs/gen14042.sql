with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        attr_key,
        attr_value
    from carts
    lateral view outer explode(attributes) x as attr_key, attr_value
),
flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        element
    from exploded
    lateral view outer explode(coupon_codes) e as element
),
enriched as (
    -- Attach customers attributes
    select
        s.*,
        d.email,
        d.city
    from flattened s
    inner join customers d
        on s.customer_id = d.id
),
enriched_carts as (
    -- Attach customers attributes
    select
        a.*,
        b.segment,
        b.age
    from enriched a
    inner join customers b
        on a.customer_id = b.id
),
scored as (
    -- Running and moving aggregates
    select
        *,
        round(age / sum(age) over (partition by element), 4) as age_share,
        avg(age) over (partition by element) as element_avg_age
    from enriched_carts
),
joined as (
    -- Attach customers attributes
    select
        a.*,
        d.age as customer_age,
        d.country,
        d.email as customer_email
    from scored a
    inner join customers d
        on a.customer_id = d.id
)
select
    s.customer_id,
    s.element,
    s.email,
    s.city,
    s.segment,
    s.age,
    s.element_avg_age,
    s.customer_age,
    s.country,
    (select min(a2.balance) from accounts a2 where a2.customer_id = s.customer_id) as min_account_balance
from joined s
order by min_account_balance desc nulls first
