with filtered as (
    -- Keep recent, valid rows
    select
        *,
        cast(quantity as double) quantity_num,
        date_add(last_restock, 11) last_restock_plus_11d
    from inventory
    where store_id in (3, 4, 6)
),
latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by store_id, product_id order by last_restock asc, id asc) as rn
    from filtered
    qualify rn = 1
),
with_dims as (
    -- Attach stores attributes
    select
        e.id,
        e.store_id,
        e.last_restock,
        e.quantity_num,
        e.last_restock_plus_11d,
        d.region
    from latest e
    inner join stores d
        on e.store_id = d.id
),
joined as (
    -- Attach stores attributes
    select
        e.id,
        e.store_id,
        e.last_restock,
        e.quantity_num,
        e.last_restock_plus_11d,
        e.region,
        s.region as store_region
    from with_dims e
    left join stores s
        on e.store_id = s.id
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(5) over (partition by region order by quantity_num asc, id asc) quantity_num_bucket,
        lead(quantity_num, 1) over (partition by store_id order by last_restock_plus_11d asc, id asc) next_quantity_num
    from joined
)
-- Grouped metrics
select
    case when next_quantity_num >= 63 then 'LARGE' else 'SMALL' end as next_quantity_num_tier,
    round(avg(quantity_num_bucket), 2) as avg_quantity_num_bucket,
    sum(case when region = 'Central' then next_quantity_num else 0 end) as central_next_quantity_num,
    avg(next_quantity_num) filter (where region = 'East') as east_avg_next_quantity_num,
    count(distinct store_region) as distinct_store_regions,
    grouping(case when next_quantity_num >= 63 then 'LARGE' else 'SMALL' end) as grouping_id
from enriched
group by cube (case when next_quantity_num >= 63 then 'LARGE' else 'SMALL' end)
having avg_quantity_num_bucket > 0
order by east_avg_next_quantity_num asc nulls last
