select
    date_trunc('year', opened_date) as year_start,
    round(sum(sqft) / nullif(count(case when city = 'Tokyo' then 1 end), 0), 2) as sqft_per_tokyo,
    stddev_samp(sqft) as sqft_stddev,
    sum(sqft) as total_sqft,
    sort_array(collect_set(region)) as region_list
from stores
group by year_start
