with recent_suppliers as (
    select *, concat(country, ' - ', supplier_name) as country_supplier_name
    from suppliers
    where lower(trim(supplier_name)) = 'tokyo traders' and country = 'JP'
),
agg as (
    select
        case when rating >= 4.5 then 'high' when rating >= 2.4 then 'mid' else 'low' end as rating_bucket,
        country,
        avg(rating) as avg_rating,
        count(rating) as rating_count
    from recent_suppliers
    group by case when rating >= 4.5 then 'high' when rating >= 2.4 then 'mid' else 'low' end, country
)
select *
from agg
where avg_rating > 2.7;
