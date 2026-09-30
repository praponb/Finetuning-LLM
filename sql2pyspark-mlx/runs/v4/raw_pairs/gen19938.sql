with base as (
    -- Basic cleansing and defaults
    select
        *,
        cast(rating as int) as rating_int,
        replace(supplier_name, ' ', '-') as supplier_name_slug,
        coalesce(city, 'NONE') as resolved_city
    from suppliers
    where supplier_name like '%Parts%'
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by city order by rating nulls last, id) as rn
        from base
    ) t
    where rn = 1
),
kept_keys as (
    -- Set difference on keys
    select id, country
    from latest
    except
    select id, country
    from suppliers
    where rating < 3.6
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    join kept_keys k on s.id = k.id and s.country = k.country
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        concat(city, ' ', supplier_name) as city_supplier_name,
        round(rating_int / rating, 2) as rating_int_per_rating
    from verified
    where supplier_name is not null
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, round(rn * 0.85, 2) as rn_adjusted
    from cleaned
    where rating between 2.3 and 4.2
),
filtered_suppliers as (
    -- Basic cleansing and defaults
    select *, rn_adjusted - rating as rn_adjusted_minus_rating
    from filtered
    where supplier_name is not null
)
select
    id,
    supplier_name,
    country,
    rating_int,
    supplier_name_slug,
    resolved_city,
    rn,
    city_supplier_name,
    rating_int_per_rating
from filtered_suppliers
order by id desc, rn;
