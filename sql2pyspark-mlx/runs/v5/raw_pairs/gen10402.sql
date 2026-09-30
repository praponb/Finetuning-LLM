with filtered_products as (
    select *
    from products
    where brand <> 'Acme' and size(tags) >= 1
),
agg as (
    select brand, category, max(launch_date) as last_launch_date
    from filtered_products
    group by brand, category
)
select brand, category, last_launch_date
from agg
order by brand, category
limit 20;
