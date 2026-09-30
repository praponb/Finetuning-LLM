with Src as (
    select *, initcap(trim(country)) as country_title
    from suppliers
    where country = 'TH' or city != 'Tokyo'
),
suppliers_stats as (
    select city, count(rating) as rating_count, sum(case when country = 'US' then rating else 0 end) as us_rating
    from Src
    group by city
)
select city, rating_count, us_rating
from suppliers_stats
order by rating_count, city;
