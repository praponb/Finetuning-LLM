with base as (
    -- Basic cleansing and defaults
    select
        id,
        country,
        city,
        rating,
        length(country) as country_length,
        coalesce(city, 'NONE') as city_filled
    from suppliers
    where city is not null
      and country = 'JP'
)
-- Latest matching product per row (LATERAL ... LIMIT 1)
select s.*, x.cost
from base s
left join lateral (
        select c.cost
        from products c
        where c.supplier_id = s.id
        order by c.launch_date desc, c.id desc
        limit 1
    ) x on true
