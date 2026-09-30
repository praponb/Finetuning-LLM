select
    case when city in ('Chiang Mai', 'Bangkok') then 'group_a' else 'other' end city_group,
    case when sqft >= 12975 then 'high' when sqft >= 4811 then 'mid' else 'low' end sqft_bucket,
    max(sqft) max_sqft,
    count(*) num_stores,
    round(stddev(sqft), 2) stddev_sqft
from stores
group by case when city in ('Chiang Mai', 'Bangkok') then 'group_a' else 'other' end, case when sqft >= 12975 then 'high' when sqft >= 4811 then 'mid' else 'low' end
having round(stddev(sqft), 2) >= 57
order by 1, sqft_bucket
limit 5;
