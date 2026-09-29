with latest as (
    select *
    from (
        select *, row_number() over (partition by country order by rating, id) as row_num
        from suppliers
    ) x
    where row_num = 1
),
not_flagged as (
    select s.*
    from latest s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.category = 'Fashion'
        )
),
valid as (
    select *, length(country) as country_length
    from not_flagged
    where city is not null
      and supplier_name is null
),
latest_suppliers as (
    select *
    from (
        select *, row_number() over (partition by country, city order by rating desc, id) as rn
        from valid
    ) x
    where rn = 1
),
by_type as (
    select *
    from (
        select
            country,
            city,
            rating
        from latest_suppliers
    ) src
    pivot (
        avg(rating) as value_sum,
        count(1) as item_count
        for city in ('Chiang Mai' as chiang_mai, 'Berlin' as berlin)
    )
)
select
    country,
    chiang_mai_value_sum,
    chiang_mai_item_count,
    berlin_value_sum,
    berlin_item_count
from by_type
order by country desc nulls last
limit 20;
