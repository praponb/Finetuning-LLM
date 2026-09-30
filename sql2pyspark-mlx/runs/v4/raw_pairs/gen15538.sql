with valid as (
    select
        *,
        initcap(trim(supplier_name)) as supplier_name_title,
        initcap(trim(city)) as city_title
    from suppliers
    where country not in ('TH', 'DE', 'SG')
)
select city, id
from valid
intersect
select city, id
from suppliers
where city = 'London'
