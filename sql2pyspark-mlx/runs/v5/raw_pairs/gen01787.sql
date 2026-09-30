with unpivoted as (
    -- Native SQL UNPIVOT (wide to long)
    select
        id,
        supplier_id,
        measure,
        metric_value
    from (
        select
            id,
            supplier_id,
            cost,
            price
        from products
    ) src
    unpivot include nulls (
        metric_value for measure in (cost as COST, price)
    )
)
-- Aggregate with conditional metrics
select
    case when metric_value >= 88 then 'HIGH' else 'LOW' end as metric_value_tier,
    count_if(measure = 'price') as price_events,
    round(sum(metric_value) / nullif(count(case when measure = 'COST' then 1 end), 0), 2) as metric_value_per_cost,
    round(sum(metric_value) / nullif(count(case when measure = 'COST' then 1 end), 0), 2) as metric_value_per_cost_2,
    approx_percentile(metric_value, array(0.25, 0.5, 0.75)) as metric_value_quartiles
from unpivoted
group by 1
order by price_events asc;
