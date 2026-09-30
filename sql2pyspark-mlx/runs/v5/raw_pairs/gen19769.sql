with deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by customer_id order by amount, id) as row_num
    from orders
    qualify row_num = 1
),
attributed as (
    -- Non-equi join on a validity window
    select
        a.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from deduped a
    inner join subscriptions sub
        on a.customer_id = sub.customer_id
       and a.order_date between sub.start_date and sub.end_date
),
joined as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.order_date,
        src.status,
        src.channel,
        src.discount,
        src.shipping_fee,
        src.subscription_id,
        src.monthly_fee,
        p.email,
        p.city,
        p.age
    from attributed src
    inner join customers p
        on src.customer_id <=> p.id
    where p.city not in ('Chiang Mai', 'Berlin')
),
latest_match as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.customer_id,
        s.order_date,
        s.channel,
        s.shipping_fee,
        s.subscription_id,
        s.monthly_fee,
        s.email,
        s.age,
        latest.unit_price
    from joined s
    join lateral (
            select c.unit_price
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
),
deduped_orders as (
    -- Keep one row per key (latest first)
    select *
    from latest_match
    qualify rank() over (partition by customer_id order by age desc) = 1
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from deduped_orders s
    where not exists (
            select 1
            from order_items x
            where x.order_id = s.id
              and x.product_id = 3
        )
)
-- Join a dimension and aggregate
select
    d.segment,
    s.channel,
    count(*) as cnt,
    min(s.shipping_fee) as min_shipping_fee,
    min(s.unit_price) as min_unit_price,
    grouping_id() as level_id
from not_flagged s
inner join customers d on s.customer_id = d.id
group by grouping sets ((d.segment, s.channel), (d.segment), ());
