with base as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        ceil(rating) as rating_whole,
        coalesce(city, 'DIRECT') as city_clean
    from suppliers
    where city is distinct from 'Bangkok'
),
remaining_keys as (
    -- Set intersection on keys
    select country, id
    from base
    intersect
    select country, id
    from suppliers
    where length(supplier_name) < 8
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    join remaining_keys k on s.country = k.country and s.id = k.id
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from verified s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.brand <> 'Acme'
        )
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from eligible s
    where not exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.cost <= 610
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.stock_qty between 148 and 151
        )
),
not_flagged_suppliers as (
    -- Anti-join via a correlated subquery
    select s.*
    from not_flagged s
    where not exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.launch_date >= '2023-08-01'
        )
)
select
    supplier_name,
    country,
    rating_whole
from not_flagged_suppliers
