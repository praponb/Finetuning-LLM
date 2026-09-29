with valid as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        quantity,
        round(unit_price * 1.1, 2) as adjusted_unit_price
    from order_items
    where product_id is not null
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by order_id order by adjusted_unit_price desc, id asc) as dedup_rank
        from valid
    ) t
    where dedup_rank = 1
),
enriched as (
    -- Attach orders attributes
    select
        e.id,
        e.order_id,
        e.quantity,
        e.dedup_rank,
        d.channel
    from deduped e
    left join orders d
        on e.order_id = d.id
),
enriched_order_items as (
    -- Window metrics over several different partitions
    select
        *,
        lag(quantity, 1) over (order by quantity asc, id asc) as prev_quantity,
        sum(quantity) over (order by dedup_rank asc, id asc rows between unbounded preceding and current row) as running_quantity
    from enriched
    where channel is not null
)
-- Join a dimension and aggregate
select
    d.channel as order_channel,
    d.status,
    e.channel as order_item_channel,
    count(*) as rows_total,
    min(e.prev_quantity) as min_prev_quantity,
    min(e.quantity) as min_quantity
from enriched_order_items e
join orders d on e.order_id = d.id
group by rollup (d.channel, d.status, e.channel)
