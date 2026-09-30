with with_dims as (
    -- Attach products attributes
    select a.*, b.brand
    from inventory a
    left join products b
        on a.product_id = b.id
    where b.product_name like 'S%'
),
scored as (
    -- Running and moving aggregates
    select
        *,
        lead(quantity) over (partition by product_id order by last_restock, id) as next_quantity,
        lag(quantity) over (partition by product_id order by last_restock, id) as prev_quantity,
        row_number() over (partition by product_id order by last_restock, id) as row_seq,
        avg(quantity) over (partition by store_id order by quantity, id rows between 3 preceding and current row) as quantity_avg_3
    from with_dims
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(quantity_avg_3, 0) as quantity_avg_3_filled,
        cast(quantity_avg_3 as double) as quantity_avg_3_num
    from scored
    where brand is not null
      and quantity between 63 and 165
),
remaining_keys as (
    -- Set intersection on keys
    select product_id
    from filtered
    intersect all
    select product_id
    from inventory
    where product_id in (2, 6)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered s
    inner join remaining_keys k on s.product_id = k.product_id
),
with_dims_inventory as (
    -- Attach stores attributes
    select
        a.id,
        a.store_id,
        a.quantity,
        a.last_restock,
        a.brand,
        a.next_quantity,
        a.prev_quantity,
        a.row_seq,
        a.quantity_avg_3,
        a.quantity_avg_3_num,
        p.city
    from reconciled a
    left join stores p
        on a.store_id = p.id
),
enriched as (
    -- Attach stores attributes
    select
        src.id,
        src.store_id,
        src.last_restock,
        src.prev_quantity,
        src.row_seq,
        src.quantity_avg_3,
        src.quantity_avg_3_num,
        src.city,
        d.region,
        d.sqft
    from with_dims_inventory src
    left join stores d
        on src.store_id = d.id
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        sqft * row_seq as sqft_times_row_seq,
        coalesce(city, 'OTHER') as city_filled
    from enriched
    where sqft < 11467
)
select
    s.id,
    s.store_id,
    s.last_restock,
    s.row_seq,
    s.quantity_avg_3,
    s.quantity_avg_3_num,
    s.city,
    s.region,
    s.sqft,
    (select sum(t2.quantity) from inventory t2 where t2.store_id = s.store_id) as sum_inventory_quantity
from valid s
