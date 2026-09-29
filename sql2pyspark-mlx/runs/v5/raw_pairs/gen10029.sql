select
    date_trunc('month', opened_date) as activity_month,
    size(collect_set(region)) as num_regions,
    percentile_approx(sqft, array(0.25, 0.5, 0.75)) as sqft_quartiles,
    approx_percentile(sqft, array(0.25, 0.5, 0.75)) as sqft_quartiles_2
from stores
group by date_trunc('month', opened_date)
having size(collect_set(region)) > 1
order by num_regions
