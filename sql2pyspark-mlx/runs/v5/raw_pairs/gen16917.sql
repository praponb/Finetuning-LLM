with filtered as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        product_id,
        last_restock,
        quarter(last_restock) as last_restock_quarter
    from inventory
    where last_restock > date '2023-04-19'
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, rank() over (partition by store_id order by last_restock_quarter desc nulls first) as dedup_rank
        from filtered
    ) x
    where dedup_rank = 1
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by product_id order by last_restock_quarter asc, id asc) as row_num
        from latest
    ) x
    where row_num = 1
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        product_id,
        last_restock_quarter,
        lag(last_restock_quarter) over (partition by store_id order by last_restock_quarter asc, id asc) as prev_last_restock_quarter,
        lead(last_restock_quarter) over (partition by store_id order by last_restock_quarter asc, id asc) as next_last_restock_quarter,
        row_number() over (partition by store_id order by last_restock_quarter asc, id asc) as event_seq
    from ranked
    where year(last_restock) >= 2025
)
select
    s.id,
    s.product_id,
    s.last_restock_quarter,
    s.prev_last_restock_quarter,
    s.next_last_restock_quarter,
    s.event_seq,
    (select max(t2.unit_price) from order_items t2 where t2.product_id = s.product_id) as max_order_item_unit_price
from metrics s
order by prev_last_restock_quarter desc
