select
    city,
    case when rating >= 3.9 then 'LARGE' else 'SMALL' end as rating_tier,
    count_if(city = 'Singapore') as singapore_events,
    stddev_samp(rating) as rating_stddev,
    grouping(city) as grouping_level
from suppliers
group by grouping sets ((city, case when rating >= 3.9 then 'LARGE' else 'SMALL' end), (city), ())
order by rating_tier desc;
