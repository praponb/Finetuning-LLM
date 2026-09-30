select
    store_id,
    sum(revenue) as total_revenue,
    count(*) as event_count,
    approx_percentile(units, 0.5) as units_p50,
    min(units) as min_units
from store_sales
group by 1
order by event_count nulls last;
