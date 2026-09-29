select
    city,
    case when rating >= 3.4 then 'high' when rating >= 1.6 then 'mid' else 'low' end rating_bucket,
    sort_array(collect_set(country)) country_list,
    round(stddev(rating), 2) stddev_rating
from suppliers
group by city, rating_bucket;
