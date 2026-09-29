select
    city,
    concat_ws(', ', sort_array(collect_set(country))) as country_values,
    round(avg(rating), 2) as avg_rating,
    round(stddev(rating), 2) as stddev_rating
from suppliers
where city not in ('Bangkok', 'Chiang Mai') and country = 'DE'
group by 1
having round(stddev(rating), 2) < 53
