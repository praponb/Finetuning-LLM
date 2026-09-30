with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(rating, 2) rating_rounded,
        coalesce(city, 'DIRECT') city_clean
    from suppliers
    where city is not null
      and country = 'UK'
),
with_last as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.price,
        x.product_cost
    from valid s
    join lateral (
            select c.price, c.cost as product_cost
            from products c
            where c.supplier_id = s.id
            order by c.launch_date desc, c.id desc
            limit 1
        ) x on true
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_last s
    where not exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.category in ('Fashion', 'Books', 'Home')
        )
),
with_last_suppliers as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.rating,
        s.city_clean,
        s.price,
        x.launch_date
    from kept s
    join lateral (
            select c.launch_date
            from products c
            where c.supplier_id = s.id
            order by c.launch_date desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    rating,
    city_clean,
    price,
    launch_date
from with_last_suppliers
order by launch_date desc;
