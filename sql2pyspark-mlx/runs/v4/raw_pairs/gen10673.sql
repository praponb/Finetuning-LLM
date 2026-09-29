select
    country,
    city,
    sum(case when city = 'New York' then rating else 0 end) as new_york_rating,
    percentile_approx(rating, array(0.25, 0.5, 0.75)) as rating_quartiles,
    stddev_samp(rating) as rating_stddev,
    size(collect_set(country)) as num_countries,
    sum(rating) as total_rating
from suppliers
group by country, city
