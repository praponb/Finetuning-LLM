with valid as (
    -- Filter the raw rows and derive helper columns
    select *, year(last_restock) as last_restock_year
    from inventory
    where last_restock >= date '2024-03-09'
      and month(last_restock) in (1, 7, 10)
),
kept_keys as (
    -- Set difference on keys
    select product_id, store_id
    from valid
    except
    select product_id, store_id
    from inventory
    where last_restock > '2024-01-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join kept_keys k on s.product_id = k.product_id and s.store_id = k.store_id
),
enriched as (
    -- Attach stores attributes
    select s.*, d.city
    from verified s
    inner join stores d
        on s.store_id = d.id
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        rank() over (partition by product_id order by quantity) as quantity_rank,
        rank() over (partition by product_id order by quantity desc nulls first) as quantity_rank_2
    from enriched
),
with_dims as (
    -- Attach products attributes
    select
        e.*,
        b.cost,
        b.category,
        b.launch_date
    from metrics e
    left join products b
        on e.product_id = b.id
)
-- Grouped metrics
select
    date_trunc('week', last_restock) as week_start,
    date_trunc('month', last_restock) as activity_month,
    bit_or(quantity) as quantity_bit_or,
    approx_percentile(quantity, 0.5) as quantity_p50,
    bit_or(quantity) as quantity_bit_or_2,
    grouping_id() as grouping_id
from with_dims
group by grouping sets ((date_trunc('week', last_restock), date_trunc('month', last_restock)), (date_trunc('week', last_restock)));
