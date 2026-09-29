select
    case when rating >= 3.8 then 'LARGE' else 'SMALL' end as rating_tier,
    count(case when city = 'London' then 1 end) as london_count,
    round(avg(rating), 2) as avg_rating,
    count(*) as row_count,
    sum(rating) as total_rating,
    size(collect_set(country)) as num_countries
from suppliers
group by 1
having round(avg(rating), 2) > 0
order by rating_tier nulls last
