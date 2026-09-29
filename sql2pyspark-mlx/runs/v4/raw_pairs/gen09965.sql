with filtered as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        product_id,
        quantity,
        case when quantity >= 188 then 'High' when quantity >= 77 then 'Medium' else 'Low' end as quantity_tier
    from inventory
    where store_id in (3, 5)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by product_id order by quantity desc, id) as rank_in_group
        from filtered
    ) t
    where rank_in_group = 1
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, round(quantity / rank_in_group, 2) as quantity_per_rank_in_group
    from latest
    where store_id in (1, 4, 5)
)
-- Multi-level aggregation
select
    case when quantity_per_rank_in_group >= 81 then 'HIGH' else 'LOW' end as quantity_per_rank_in_group_tier,
    stddev_samp(quantity) as quantity_stddev,
    round(avg(quantity_per_rank_in_group), 2) as avg_quantity_per_rank_in_group,
    max(quantity_per_rank_in_group) as max_quantity_per_rank_in_group,
    avg(quantity) as avg_quantity
from valid
group by quantity_per_rank_in_group_tier
