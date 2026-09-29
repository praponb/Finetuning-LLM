with lines as (
    -- Unnest the array into one row per element
    select
        id,
        coupon_codes,
        key,
        attr_value
    from carts
    lateral view explode(attributes) e as key, attr_value
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        upper(key) as key_upper,
        coalesce(key, 'DIRECT') as resolved_key
    from lines
    where key is not null
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        sort_array(coupon_codes) as sorted_coupon_codes,
        coalesce(key, 'UNKNOWN') as resolved_key_2
    from valid
    where array_contains(coupon_codes, 'SPRING10')
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        coupon_codes,
        key,
        resolved_key,
        length(key) as key_length,
        coalesce(key, 'NONE') as key_filled
    from base
    where array_contains(coupon_codes, 'WELCOME5')
)
-- Multi-level aggregation
select
    case when key_length >= 69 then 'HIGH' else 'LOW' end as key_length_tier,
    key,
    count(distinct id) as distinct_ids,
    round(sum(key_length) / nullif(count(case when key = 'gift' then 1 end), 0), 2) as key_length_per_gift,
    approx_percentile(key_length, array(0.25, 0.5, 0.75)) as key_length_quartiles,
    avg(key_length) filter (where key = 'gift') as gift_avg_key_length,
    grouping_id() as gid
from filtered
group by rollup (case when key_length >= 69 then 'HIGH' else 'LOW' end, key)
having round(sum(key_length) / nullif(count(case when key = 'gift' then 1 end), 0), 2) >= 0
order by key_length_per_gift;
