with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        upper(region) as region_upper,
        lower(store_name) as store_name_lower
    from stores
    where opened_date > date '2023-05-25'
      and region is not null
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by city, region order by opened_date desc, id) as row_seq,
        first_value(sqft, true) over (partition by city order by opened_date, id rows between unbounded preceding and current row) as first_known_sqft,
        max(sqft) over (partition by city) as city_max_sqft,
        max(sqft) over (partition by region) as region_max_sqft,
        first_value(sqft, true) over (partition by city order by sqft, id rows between unbounded preceding and current row) as first_known_sqft_2
    from valid
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        lead(row_seq, 1) over (partition by city order by first_known_sqft, id) as next_row_seq,
        first_value(sqft, true) over (order by region_max_sqft, id rows between unbounded preceding and current row) as first_known_sqft_3
    from metrics
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from scored
    qualify row_number() over (partition by city order by first_known_sqft desc nulls last, id) = 1
)
-- Native SQL PIVOT
select *
from (
    select
        city,
        region,
        region_max_sqft
    from deduped
) src
pivot (
    sum(region_max_sqft)
    for region in ('East', 'West')
)
