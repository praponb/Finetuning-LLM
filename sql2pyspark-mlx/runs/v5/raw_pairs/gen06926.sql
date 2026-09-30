with valid as (
    -- Basic cleansing and defaults
    select
        *,
        case when country = 'DE' then 1 else 0 end as is_de,
        coalesce(city, 'UNKNOWN') as city_filled
    from suppliers
    where country = 'JP'
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from valid
    qualify rank() over (partition by country order by is_de nulls first) = 1
),
matched_keys as (
    -- Set intersection on keys
    select city, id
    from deduped
    intersect all
    select city, id
    from suppliers
    where country in ('TH', 'US')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    join matched_keys k on s.city = k.city and s.id = k.id
),
cleaned as (
    -- Keep recent, valid rows
    select
        supplier_name,
        city,
        rating,
        is_de,
        case when country = 'UK' then 1 else 0 end as is_uk
    from reconciled
    where rating >= 2.7
)
select
    supplier_name,
    city,
    rating,
    is_uk
from cleaned
order by rating nulls first;
