with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        length(city) as city_length,
        coalesce(region, 'DIRECT') as resolved_region
    from stores
    where region is not null
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by city order by sqft nulls first, id) as row_num
        from filtered
    ) x
    where row_num = 1
),
cleaned as (
    -- Keep recent, valid rows
    select *, lower(store_name) as store_name_lower
    from deduped
    where region is not null
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        city_length,
        resolved_region,
        row_num,
        store_name_lower,
        avg(sqft) over (order by opened_date, id rows between 2 preceding and current row) as sqft_avg_2,
        lag(sqft, 2) over (order by city_length, id) as prev_sqft
    from cleaned
)
select
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    city_length,
    resolved_region,
    row_num
from with_windows
order by region nulls last;
