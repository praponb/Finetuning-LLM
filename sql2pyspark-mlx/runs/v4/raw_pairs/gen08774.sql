select
    case when quantity >= 250 then 'PREMIUM' else 'STANDARD' end as quantity_tier,
    approx_percentile(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
    stddev_samp(quantity) as quantity_stddev,
    count(*) as event_count,
    percentile_approx(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles_2,
    approx_percentile(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles_3,
    round(avg(quantity), 2) as avg_quantity
from inventory
group by quantity_tier
order by avg_quantity desc nulls last;
