with valid as (
    select *, concat_ws('-', country, city) as country_city_key
    from suppliers
    where rating < 3.4
),
valid_suppliers as (
    select
        id,
        supplier_name,
        country,
        city,
        country_city_key,
        coalesce(city, 'unassigned') as city_filled,
        coalesce(city, 'UNKNOWN') as resolved_city
    from valid
    where length(supplier_name) > 9
),
kept as (
    select s.*
    from valid_suppliers s
    where not exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.cost < 220
        )
)
select
    id,
    supplier_name,
    city,
    city_filled
from kept
order by id desc
limit 10
