with with_window as (
    -- Non-equi join on a validity window
    select
        e.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from orders e
    left join subscriptions sub
        on e.customer_id = sub.customer_id
       and e.order_date between sub.start_date and sub.end_date
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_window s
    where exists (
            select 1
            from payments x
            where x.order_id = s.id
              and x.paid_at <= s.order_date
        )
),
latest_match as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.order_item_unit_price,
        x.order_item_quantity
    from eligible s
    left join lateral (
            select c.unit_price as order_item_unit_price, c.quantity as order_item_quantity
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from latest_match
    qualify row_number() over (partition by status order by discount desc nulls last, id, subscription_id) = 1
),
with_latest as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select s.*, latest.order_item_unit_price_2
    from latest s
    join lateral (
            select c.unit_price as order_item_unit_price_2
            from order_items c
            where c.order_id = s.id
            order by c.unit_price desc, c.id desc
            limit 1
        ) latest on true
)
-- Multi-level aggregation
select
    case when monthly_fee >= 222 then 'PREMIUM' else 'STANDARD' end as monthly_fee_tier,
    stddev_samp(discount) as discount_stddev,
    sort_array(collect_set(plan)) as plan_list,
    count_if(channel = 'store') as store_events
from with_latest
group by case when monthly_fee >= 222 then 'PREMIUM' else 'STANDARD' end
order by store_events desc nulls last
