with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        attr_key,
        value
    from carts
    lateral view explode(attributes) t as attr_key, value
),
exploded_carts as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        attr_key,
        explode_outer(coupon_codes) as code
    from exploded
),
enriched as (
    -- Attach customers attributes
    select s.*, c.name
    from exploded_carts s
    left join customers c
        on s.customer_id <=> c.id
),
enriched_carts as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.code,
        a.name,
        c.age
    from enriched a
    inner join customers c
        on a.customer_id = c.id
),
metrics as (
    -- Window metrics over several different partitions
    select *, min(age) over (partition by customer_id, code) as customer_id_min_age
    from enriched_carts
),
daily as (
    -- Aggregate with conditional metrics
    select
        customer_id,
        avg(age) filter (where code = 'BUNDLE') as bundle_avg_age,
        count_if(code = 'VIP') as vip_events,
        count(distinct id) as distinct_ids,
        avg(customer_id_min_age) filter (where code = 'BUNDLE') as bundle_avg_customer_id_min_age,
        count_if(code = 'WELCOME5') as welcome5_events,
        count(distinct customer_id) as distinct_customer_ids,
        grouping(customer_id) as grouping_id
    from metrics
    group by cube (customer_id)
    having bundle_avg_age < 1000000
)
select
    s.customer_id,
    s.bundle_avg_age,
    s.vip_events,
    s.distinct_ids,
    s.distinct_customer_ids,
    s.grouping_id,
    (select sum(t2.shipping_fee) from orders t2 where t2.customer_id = s.customer_id) as sum_order_shipping_fee
from daily s
order by bundle_avg_age desc nulls last;
