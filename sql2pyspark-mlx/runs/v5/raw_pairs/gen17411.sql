select
    city,
    country,
    count(distinct rating) as unique_citys,
    count(rating) as rating_count,
    concat_ws(', ', sort_array(collect_set(country))) as country_values
from suppliers
group by city, country
having count(distinct rating) > 3
