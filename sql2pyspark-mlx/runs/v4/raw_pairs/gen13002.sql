with valid as (
    select
        *,
        coalesce(city, 'N/A') as city_filled,
        substring(country, 1, 4) as country_prefix
    from suppliers
    where city is not null
      and supplier_name like '%EuroGoo%'
),
base as (
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        city_filled,
        country_prefix,
        trim(city) as clean_city
    from valid
    where city is not null
),
ranked as (
    select *
    from (
        select *, rank() over (partition by country order by rating asc nulls last) as latest_rank
        from base
    ) x
    where latest_rank = 1
),
with_latest as (
    select s.*, top1.product_category
    from ranked s
    join lateral (
            select c.category as product_category
            from products c
            where c.supplier_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) top1 on true
)
select
    id,
    supplier_name,
    country,
    city,
    country_prefix,
    clean_city,
    latest_rank,
    case when supplier_name rlike '^(Global Supply|EuroGoods GmbH)$' then 'OTHER_ZONE' when supplier_name rlike '^(Siam Parts Co|Tokyo Traders)$' then 'EXTENDED' else 'GROUP_B' end as supplier_name_zone
from with_latest
order by latest_rank desc nulls first
