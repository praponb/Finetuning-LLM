select
    date_trunc('week', sale_date) as week_start,
    case when revenue >= 3800 then 'HIGH' else 'LOW' end as revenue_tier,
    case when units >= 31 then 'PREMIUM' else 'STANDARD' end as units_tier,
    count(*) as cnt,
    approx_percentile(units, 0.9) as units_p90,
    corr(revenue, units) as corr_revenue_units,
    corr(units, revenue) as corr_units_revenue,
    approx_percentile(revenue, 0.95) as revenue_p95
from store_sales
group by date_trunc('week', sale_date), case when revenue >= 3800 then 'HIGH' else 'LOW' end, case when units >= 31 then 'PREMIUM' else 'STANDARD' end
order by units_p90 asc;
