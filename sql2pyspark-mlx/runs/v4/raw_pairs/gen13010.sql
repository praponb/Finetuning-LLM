select
    case when units >= 33 then 'LARGE' else 'SMALL' end as units_tier,
    percentile_approx(units, 0.95) as units_p95,
    sum(units) as total_units,
    stddev_samp(revenue) as revenue_stddev,
    stddev_samp(units) as units_stddev,
    stddev_samp(units) as units_stddev_2
from store_sales
group by rollup (case when units >= 33 then 'LARGE' else 'SMALL' end)
order by units_p95 desc
