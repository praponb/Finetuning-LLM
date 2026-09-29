with valid_suppliers as (
    select *, initcap(trim(country)) as country_title
    from suppliers
    where country = 'UK'
),
suppliers_stats as (
    select case when rating >= 3.4 then 'high' when rating >= 2.5 then 'mid' else 'low' end as rating_bucket, min(rating) as min_rating, sum(case when city = 'Singapore' then 1 else 0 end) as singapore_count
    from valid_suppliers
    group by case when rating >= 3.4 then 'high' when rating >= 2.5 then 'mid' else 'low' end
)
select rating_bucket, min_rating, singapore_count
from suppliers_stats
