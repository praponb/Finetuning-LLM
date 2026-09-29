select
    country,
    stddev_samp(rating) as rating_stddev,
    round(avg(rating), 2) as avg_rating,
    size(collect_set(city)) as num_cities,
    count(*) as cnt
from suppliers
group by country
order by country desc nulls first
