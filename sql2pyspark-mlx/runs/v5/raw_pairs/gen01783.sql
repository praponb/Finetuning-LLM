with recent_products as (
    select *
    from products
    where brand = 'Umbrella'
),
products_stats as (
    select
        category,
        brand,
        max(price) - min(price) as price_range,
        sum(case when category = 'Fashion' then 1 else 0 end) as fashion_count
    from recent_products
    group by category, brand
)
select category, brand, price_range
from products_stats
where price_range < 31
