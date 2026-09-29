with recent_suppliers as (
    select *
    from suppliers
    where supplier_name is not null and country = 'SG'
),
suppliers_stats as (
    select case when rating >= 4.2 then 'high' when rating >= 1.6 then 'mid' else 'low' end as rating_bucket, round(stddev(rating), 2) as stddev_rating, count(case when city = 'Bangkok' then 1 end) as bangkok_count
    from recent_suppliers
    group by case when rating >= 4.2 then 'high' when rating >= 1.6 then 'mid' else 'low' end
)
select rating_bucket, stddev_rating, bangkok_count
from suppliers_stats
order by stddev_rating;
