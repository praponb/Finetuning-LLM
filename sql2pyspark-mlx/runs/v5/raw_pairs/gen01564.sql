with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        city,
        round(rating, 2) as rating_rounded,
        upper(trim(supplier_name)) as supplier_name_normalized,
        coalesce(city, 'NONE') as resolved_city
    from suppliers
    where rating < 3.9
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and year(c.launch_date) = 2025
        )
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        city,
        resolved_city,
        last_value(rating_rounded) ignore nulls over (partition by city order by rating_rounded, id rows between unbounded preceding and current row) as last_known_rating_rounded,
        max(rating_rounded) over (partition by city) as city_max_rating_rounded
    from eligible
),
with_last as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        latest.product_launch_date,
        latest.product_cost
    from with_windows s
    join lateral (
            select c.launch_date as product_launch_date, c.cost as product_cost
            from products c
            where c.supplier_id = s.id
            order by c.launch_date desc, c.id desc
            limit 1
        ) latest on true
)
select
    id,
    city,
    product_cost,
    case when city rlike '^(Singapore|Tokyo)$' then 'CORE' else 'EXTENDED' end as city_zone
from with_last
order by city_zone, city desc;
