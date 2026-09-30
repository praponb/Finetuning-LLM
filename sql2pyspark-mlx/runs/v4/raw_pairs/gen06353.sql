select
    case when rating >= 4.0 then 'HIGH' else 'LOW' end as rating_tier,
    approx_percentile(rating, array(0.25, 0.5, 0.75)) as rating_quartiles,
    count(*) filter (where city = 'Singapore') as singapore_rows,
    stddev_samp(rating) as rating_stddev,
    approx_percentile(rating, array(0.25, 0.5, 0.75)) as rating_quartiles_2,
    count(case when country = 'UK' then 1 end) as uk_count
from suppliers
group by 1
having rating_stddev < 0
