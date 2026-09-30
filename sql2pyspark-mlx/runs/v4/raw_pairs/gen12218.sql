select
    case when rating >= 4.4 then 'high' when rating >= 2.5 then 'mid' else 'low' end as rating_bucket,
    country,
    min(rating) as min_rating,
    round((100 * sum(case when city = 'New York' then 1 else 0 end)) / count(*), 1) as pct_new_york
from suppliers
group by cube(case when rating >= 4.4 then 'high' when rating >= 2.5 then 'mid' else 'low' end, country)
