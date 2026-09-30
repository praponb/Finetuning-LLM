with valid as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        round(cost * 0.9, 2) as cost_adjusted
    from shipments
    where order_id is not null
      and delivered_date between '2024-09-19' and '2025-01-11'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_adjusted,
        ntile(5) over (partition by order_id order by cost desc, id) as cost_bucket,
        row_number() over (partition by order_id order by delivered_date desc, id) as seq_no,
        row_number() over (partition by order_id order by delivered_date, id) as seq_no_2
    from valid
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by status, order_id order by seq_no_2 desc nulls last, id) as dedup_rank
    from with_windows
    qualify dedup_rank = 1
)
select
    s.id,
    s.order_id,
    s.shipped_date,
    s.delivered_date,
    s.cost_adjusted,
    s.seq_no,
    s.seq_no_2,
    (select max(t2.quantity) from order_items t2 where t2.order_id = s.order_id) as max_order_item_quantity
from deduped s
order by cost_adjusted desc
