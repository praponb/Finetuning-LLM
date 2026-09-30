with base as (
    -- Basic cleansing and defaults
    select *, regexp_replace(supplier_name, ' ', '') as supplier_name_slug
    from suppliers
    where city is not null
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        substring(city, 1, 2) as city_prefix,
        coalesce(city, 'OTHER') as city_filled
    from base
    where city is not null
      and lower(trim(supplier_name)) = 'lion city imports'
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from cleaned s
    where exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.category = 'Grocery'
        )
),
with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select s.*, latest.stock_qty
    from not_flagged s
    join lateral (
            select c.stock_qty
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) latest on true
)
-- Grouped metrics
select
    city,
    case when rating >= 3.8 then 'PREMIUM' else 'STANDARD' end as rating_tier,
    country,
    count(*) filter (where country = 'US') as us_rows,
    count_if(country = 'SG') as sg_events,
    percentile_approx(rating, array(0.25, 0.5, 0.75)) as rating_quartiles
from with_latest
group by city, case when rating >= 3.8 then 'PREMIUM' else 'STANDARD' end, country;
