select
    case when city in ('Singapore', 'Chiang Mai') then 'core' else 'other' end as city_group,
    case when rating >= 4.5 then 'high' when rating >= 1.6 then 'mid' else 'low' end as rating_bucket,
    max(rating) - min(rating) as rating_range,
    sum(rating) as total_rating
from suppliers
where rating < 4.1
group by 1, 2
order by city_group;
