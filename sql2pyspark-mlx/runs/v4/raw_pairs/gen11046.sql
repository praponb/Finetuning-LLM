with exploded as (
    select
        id,
        customer_id,
        tag
    from carts
    lateral view explode(coupon_codes) e as tag
),
enriched as (
    select
        src.*,
        d.name,
        d.email
    from exploded src
    join customers d
        on src.customer_id = d.id
),
kept_keys as (
    select customer_id, id
    from enriched
    except distinct
    select customer_id, id
    from carts
    where hour(created_at) between 7 and 12
),
verified as (
    select s.*
    from enriched s
    join kept_keys k on s.customer_id = k.customer_id and s.id = k.id
),
enriched_carts as (
    select
        a.*,
        b.city,
        b.email as customer_email
    from verified a
    left join customers b
        on a.customer_id = b.id
),
with_dims as (
    select
        a.*,
        b.country,
        b.city as customer_city
    from enriched_carts a
    left join customers b
        on a.customer_id = b.id
),
filtered as (
    select
        id,
        customer_id,
        tag,
        email,
        city,
        customer_email,
        country,
        customer_city,
        trim(email) as clean_email,
        substring(tag, 1, 3) as tag_prefix
    from with_dims
    where email like '%@yahoo.com'
)
select
    id,
    tag,
    email,
    city,
    customer_email,
    country,
    customer_city,
    tag_prefix
from filtered
order by customer_city desc nulls last
