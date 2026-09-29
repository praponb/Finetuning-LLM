with cleaned as (
    -- Keep recent, valid rows
    select *, date_add(last_restock, 24) as last_restock_plus_24d
    from inventory
    where last_restock >= to_date('2023-03-01')
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        last_restock_plus_24d,
        round(quantity * 1.15, 2) as adjusted_quantity
    from cleaned
    where last_restock > date '2023-10-14'
      and year(last_restock) >= 2023
),
remaining_keys as (
    -- Set difference on keys
    select store_id, product_id
    from valid
    except
    select store_id, product_id
    from inventory
    where quantity between 79 and 108
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join remaining_keys k on s.store_id = k.store_id and s.product_id = k.product_id
),
with_dims as (
    -- Attach products attributes
    select src.*, b.brand
    from kept_rows src
    left join products b
        on src.product_id = b.id
)
-- Multi-level aggregation
select
    date_trunc('week', last_restock_plus_24d) as week_start,
    percentile_approx(adjusted_quantity, 0.9) as adjusted_quantity_p90,
    round(sum(adjusted_quantity) / nullif(count(case when brand = 'Stark' then 1 end), 0), 2) as adjusted_quantity_per_stark,
    sum(case when brand = 'Umbrella' then adjusted_quantity else 0 end) as umbrella_adjusted_quantity,
    round(sum(adjusted_quantity) / nullif(count(case when brand = 'Stark' then 1 end), 0), 2) as adjusted_quantity_per_stark_2,
    count_if(brand = 'Initech') as initech_events
from with_dims
group by date_trunc('week', last_restock_plus_24d)
having adjusted_quantity_per_stark_2 > 0
