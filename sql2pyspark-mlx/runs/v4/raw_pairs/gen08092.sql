with RecentSuppliers as (
    select *
    from suppliers
    where country = 'SG'
),
suppliers_stats as (
    select country, sum(case when country = 'US' then rating else 0 end) us_rating, sum(case when city = 'Chiang Mai' then 1 else 0 end) chiang_mai_count
    from RecentSuppliers
    group by country
)
select country, us_rating
from suppliers_stats
order by us_rating desc
