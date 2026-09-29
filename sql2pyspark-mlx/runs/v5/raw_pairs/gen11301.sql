with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        initcap(trim(country)) as country_title,
        coalesce(city, 'UNKNOWN') as city_clean
    from suppliers
    where city is distinct from 'Chiang Mai'
      and city in ('Chiang Mai', 'Tokyo', 'Singapore')
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        city,
        rating,
        country_title,
        count(rating) over (partition by city, country order by rating, id rows between 6 preceding and current row) as last7_count_rating,
        min(rating) over (partition by city, country order by rating, id rows between 6 preceding and current row) as last7_min_rating,
        max(rating) over (partition by city, country order by rating, id rows between 6 preceding and current row) as last7_max_rating,
        lag(rating, 1) over (order by rating, id) as prev_rating
    from cleaned
    where country = 'JP'
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        city,
        rating,
        country_title,
        last7_min_rating,
        last7_max_rating,
        prev_rating,
        rating + last7_min_rating as rating_plus_last7_min_rating,
        coalesce(city, 'OTHER') as city_filled
    from enriched
    where city is not null
),
with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        s.rating,
        s.country_title,
        s.last7_max_rating,
        s.prev_rating,
        s.rating_plus_last7_min_rating,
        s.city_filled,
        x.product_brand,
        x.cost
    from filtered s
    left join lateral (
            select c.brand as product_brand, c.cost
            from products c
            where c.supplier_id = s.id
            order by c.stock_qty desc, c.id desc
            limit 1
        ) x on true
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_latest s
    where exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.supplier_id is not null
        )
)
-- Multi-level aggregation
select
    city,
    count(*) as row_count,
    sum(cost) filter (where city = 'Chiang Mai') as chiang_mai_total_cost
from not_flagged
group by city;
