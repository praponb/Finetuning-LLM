with lines as (
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        rec.product_id,
        rec.qty,
        rec.price
    from carts
    lateral view explode(items) e as rec
),
enriched as (
    select
        src.*,
        b.name,
        b.segment,
        b.signup_date
    from lines src
    left join customers b
        on src.customer_id = b.id
),
joined as (
    select
        s.customer_id,
        s.coupon_codes,
        s.product_id,
        s.qty,
        s.name,
        s.segment,
        s.signup_date,
        p.email
    from enriched s
    left join customers p
        on s.customer_id = p.id
),
enriched_carts as (
    select
        src.*,
        c.email as customer_email,
        c.segment as customer_segment
    from joined src
    left join customers c
        on src.customer_id = c.id
    where c.is_active = false
),
joined_carts as (
    select
        a.customer_id,
        a.coupon_codes,
        a.product_id,
        a.signup_date,
        a.email,
        a.customer_segment,
        b.segment as customer_segment_2
    from enriched_carts a
    join customers b
        on a.customer_id = b.id
),
joined_carts_2 as (
    select
        a.customer_id,
        a.product_id,
        a.customer_segment_2,
        d.age,
        d.credit_limit,
        d.country
    from joined_carts a
    left join customers d
        on a.customer_id = d.id
)
select
    d.country as customer_country,
    d.city,
    s.customer_segment_2,
    count(*) as rows_total,
    max(s.credit_limit) as max_credit_limit,
    max(s.product_id) as max_product_id
from joined_carts_2 s
join customers d on s.customer_id = d.id
group by grouping sets ((d.country, d.city, s.customer_segment_2), (d.country, d.city), (d.country))
