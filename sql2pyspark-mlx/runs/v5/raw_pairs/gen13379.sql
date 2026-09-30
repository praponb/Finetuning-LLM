with base as (
    -- Keep recent, valid rows
    select
        *,
        round(shipping_fee, 2) as shipping_fee_rounded,
        coalesce(channel, 'NONE') as channel_filled
    from orders
    where store_id is not null
      and order_date >= date '2023-07-08'
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        order_date,
        amount,
        discount,
        shipping_fee,
        shipping_fee_rounded,
        lead(discount) over (partition by customer_id order by amount, id) as next_discount,
        lag(discount) over (partition by customer_id order by amount, id) as prev_discount,
        lag(shipping_fee) over (partition by customer_id order by amount, id) as prev_shipping_fee,
        lead(shipping_fee) over (partition by customer_id order by amount, id) as next_shipping_fee,
        row_number() over (partition by customer_id order by amount, id) as event_seq,
        avg(discount) over (partition by store_id, customer_id order by shipping_fee, id rows between 3 preceding and current row) as discount_avg_3
    from base
    where discount >= 0.37
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from payments c
            where c.order_id = s.id
              and c.order_id is not null
        )
),
base_orders as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(next_discount, 0) as next_discount_filled,
        coalesce(shipping_fee, 0) as shipping_fee_filled
    from not_flagged
    where discount < 0.38
),
base_orders_2 as (
    -- Filter the raw rows and derive helper columns
    select *, least(next_discount_filled, discount_avg_3) as least_next_discount_filled_discount_avg_3
    from base_orders
    where order_date >= date '2023-09-05'
      and shipping_fee_rounded > 36
),
daily as (
    -- Multi-level aggregation
    select
        case when discount >= 0.34 then 'HIGH' else 'LOW' end as discount_tier,
        case when prev_discount >= 0.42 then 'HIGH' else 'LOW' end as prev_discount_tier,
        date_trunc('quarter', order_date) as quarter_start,
        count(distinct id) as distinct_ids,
        sum(next_shipping_fee) as total_next_shipping_fee,
        count(distinct id) as distinct_ids_2
    from base_orders_2
    group by discount_tier, prev_discount_tier, quarter_start
)
select
    discount_tier,
    prev_discount_tier,
    quarter_start,
    distinct_ids,
    total_next_shipping_fee,
    distinct_ids_2
from daily
