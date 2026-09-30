with base as (
    select *
    from suppliers
    where city in ('Singapore', 'Chiang Mai') and (rating <= 3.4 or city = 'New York')
),
suppliers_stats as (
    select
        case when city in ('Chiang Mai', 'Singapore') then 'priority' else 'other' end as city_group,
        case when rating >= 3.4 then 'high' when rating >= 2.0 then 'mid' else 'low' end as rating_bucket,
        count(*) as cnt,
        avg(rating) as avg_rating
    from base
    group by case when city in ('Chiang Mai', 'Singapore') then 'priority' else 'other' end, case when rating >= 3.4 then 'high' when rating >= 2.0 then 'mid' else 'low' end
)
select *
from suppliers_stats
where cnt < 3
order by avg_rating desc, cnt desc;
