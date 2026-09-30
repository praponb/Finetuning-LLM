with base as (
    -- Filter the raw rows and derive helper columns
    select *, concat_ws('/', city, supplier_name) as city_supplier_name_key
    from suppliers
    where country not in ('SG', 'US', 'DE')
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from base s
    where exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.product_name like 'Yog%'
        )
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by country order by rating nulls last, id) as latest_rank
        from eligible
    ) t
    where latest_rank = 1
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by city, country order by latest_rank desc, id) as rn
    from ranked
    qualify rn = 1
)
select
    id,
    country,
    rating,
    city_supplier_name_key,
    latest_rank,
    rn
from deduped
order by id nulls first
limit 20
