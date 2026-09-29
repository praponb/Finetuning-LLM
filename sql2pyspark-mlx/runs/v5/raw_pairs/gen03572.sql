with kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from suppliers s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.category in ('Fashion', 'Electronics')
        )
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.launch_date > '2024-05-01'
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from eligible s
    where not exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.launch_date >= '2025-07-27'
        )
),
with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.supplier_name,
        s.country,
        s.city,
        x.product_brand
    from not_flagged s
    join lateral (
            select c.brand as product_brand
            from products c
            where c.supplier_id = s.id
            order by c.stock_qty desc, c.id desc
            limit 1
        ) x on true
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        coalesce(city, 'Unknown') city_filled,
        concat(product_brand, ' - ', country) product_brand_country
    from with_latest
    where city is not null
      and country in ('US', 'DE')
),
kept_suppliers as (
    -- Anti-join via a correlated subquery
    select s.*
    from cleaned s
    where not exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.stock_qty >= 192
        )
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat_ws(',', country, supplier_name) country_supplier_name_key,
        coalesce(city, 'UNKNOWN') city_clean
    from kept_suppliers
    where product_brand is not null
      and country != 'UK'
)
select
    id,
    supplier_name,
    country,
    city,
    product_brand,
    product_brand_country,
    country_supplier_name_key,
    city_clean
from valid
order by city desc nulls first;
