with base as (
    -- Keep recent, valid rows
    select *, coalesce(rating, 0) as rating_filled
    from suppliers
    where supplier_name like 'P%'
),
cleaned as (
    -- Keep recent, valid rows
    select *, substring(supplier_name, 1, 2) as supplier_name_prefix
    from base
    where city is not null
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        city,
        rating_filled,
        supplier_name_prefix,
        upper(trim(supplier_name)) as supplier_name_normalized
    from cleaned
    where country = 'TH'
),
ranked as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by city order by rating_filled desc) as latest_rank
    from filtered
    qualify latest_rank = 1
),
valid as (
    -- Keep recent, valid rows
    select *, case when rating_filled >= 68 then 'High' when rating_filled >= 23 then 'Medium' else 'Low' end as rating_filled_band
    from ranked
    where city is not null
      and city is distinct from 'Chiang Mai'
)
select
    id,
    city,
    rating_filled,
    supplier_name_prefix,
    supplier_name_normalized,
    rating_filled_band
from valid
order by rating_filled nulls last;
