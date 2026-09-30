with ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by store_id, product_id order by quantity, id) as rn
        from inventory
    ) ranked
    where rn = 1
)
-- Aggregate with conditional metrics
select
    case when quantity >= 229 then 'LARGE' else 'SMALL' end as quantity_tier,
    round(avg(quantity), 2) as avg_quantity,
    percentile_approx(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
    count(*) as row_count
from ranked
group by quantity_tier
having avg_quantity >= 0
