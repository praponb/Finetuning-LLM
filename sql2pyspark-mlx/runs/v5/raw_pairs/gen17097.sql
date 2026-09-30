with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    from carts
    lateral view explode(attributes) x as key, value
),
flattened as (
    -- Unnest the array into one row per element
    select
        id,
        key,
        value,
        value_2
    from exploded
    lateral view explode(coupon_codes) t as value_2
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        concat(key, ' ', value_2) as key_value_2,
        coalesce(value_2, 'DIRECT') as value_2_filled
    from flattened
    where key <> 'device'
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        upper(value_2) as value_2_upper,
        coalesce(key, 'N/A') as key_filled
    from filtered
    where value_2 = 'WELCOME5'
),
valid_carts as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        substring(value_2, 1, 4) as value_2_prefix,
        lower(key) as key_lower,
        coalesce(value_2, 'DIRECT') as value_2_filled_2
    from valid
    where value_2 <> 'SPRING10'
),
filtered_carts as (
    -- Basic cleansing and defaults
    select
        id,
        key,
        value,
        value_2,
        key_value_2,
        value_2_filled_2,
        concat_ws('-', key, value_2) as key_value_2_key
    from valid_carts
    where key in ('utm_source', 'gift', 'weight_kg')
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when key in ('utm_source', 'gift') then 'Group A' else 'Other' end as key_group,
        coalesce(key, 'NONE') as key_filled
    from filtered_carts
    where key is not null
)
-- Multi-level aggregation
select
    value_2,
    size(collect_set(value_2)) as num_value_2s,
    count(case when value_2 = 'BUNDLE' then 1 end) as bundle_count,
    count_if(key = 'weight_kg') as weight_kg_events
from base
group by 1
order by bundle_count desc nulls first;
