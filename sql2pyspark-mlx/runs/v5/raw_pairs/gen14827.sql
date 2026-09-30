with deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        customer_id,
        store_id,
        amount,
        shipping_fee
    from (
        select *, row_number() over (partition by customer_id order by amount nulls first, id) as rn
        from orders
    ) x
    where rn = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        lag(amount, 1) over (partition by customer_id order by amount, id) as prev_amount,
        cume_dist() over (order by amount) as amount_cume_dist
    from deduped
),
deduped_orders as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by customer_id order by prev_amount desc, id) as rn
    from metrics
    qualify rn = 1
),
with_last as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select s.*, latest.shipment_cost
    from deduped_orders s
    left join lateral (
            select c.cost as shipment_cost
            from shipments c
            where c.order_id = s.id
            order by c.delivered_date desc, c.id desc
            limit 1
        ) latest on true
)
select
    id,
    customer_id,
    amount,
    shipping_fee,
    prev_amount,
    amount_cume_dist,
    rn
from with_last
order by rn, amount
