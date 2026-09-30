with valid as (
    select *, concat(supplier_name, ' - ', city) as supplier_name_city
    from suppliers
    where city is not null
      and city is not distinct from 'New York'
)
select *
from (
    select
        country,
        city,
        rating
    from valid
) src
pivot (
    sum(rating)
    for city in ('New York' as new_york, 'Chiang Mai' as chiang_mai)
)
