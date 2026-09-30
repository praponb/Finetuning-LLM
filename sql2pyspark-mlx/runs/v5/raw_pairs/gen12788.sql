with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        explode_outer(coupon_codes) as code
    from carts
),
with_dims as (
    -- Attach customers attributes
    select src.*, p.email
    from exploded src
    left join customers p
        on src.customer_id = p.id
),
enriched as (
    -- Attach customers attributes
    select
        s.*,
        d.credit_limit,
        d.country,
        d.email as customer_email
    from with_dims s
    left join customers d
        on s.customer_id = d.id
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        concat_ws('-', customer_email, code) customer_email_code_key,
        coalesce(country, 'unassigned') country_filled,
        coalesce(code, 'OTHER') code_filled
    from enriched
    where customer_id is not null
),
joined as (
    -- Attach customers attributes
    select
        src.customer_id,
        src.credit_limit,
        src.customer_email_code_key,
        d.age,
        d.credit_limit as customer_credit_limit,
        d.email as customer_email_2
    from valid src
    left join customers d
        on src.customer_id = d.id
)
-- Join a dimension and aggregate
select
    dim.segment,
    count(*) as cnt,
    min(s.credit_limit) as min_credit_limit,
    avg(s.customer_credit_limit) as avg_customer_credit_limit
from joined s
join customers dim on s.customer_id = dim.id
group by dim.segment
order by cnt desc nulls first
