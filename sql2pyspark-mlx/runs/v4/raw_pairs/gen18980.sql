select
    city,
    country,
    jp_rating,
    rating_count
from (
    select
        city,
        country,
        sum(case when country = 'JP' then rating else 0 end) as jp_rating,
        count(rating) as rating_count
    from suppliers
    where rating <= 2.2
    group by city, country
) as agg
where jp_rating > 67
order by rating_count desc, city, country
limit 10
