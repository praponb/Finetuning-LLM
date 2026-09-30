with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        attr_value
    from carts
    lateral view outer explode(attributes) li as key, attr_value
),
flattened_carts as (
    -- Unnest the array into one row per element
    select
        id,
        key,
        attr_value,
        element
    from flattened
    lateral view outer explode(coupon_codes) e as element
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        length(key) as key_length,
        coalesce(key, 'DIRECT') as key_filled
    from flattened_carts
    where element is not null
      and element is not distinct from 'BUNDLE'
),
metrics as (
    -- Running and moving aggregates
    select
        key,
        attr_value,
        key_length,
        key_filled,
        sum(key_length) over (partition by element) as element_sum_key_length,
        cume_dist() over (partition by key order by key_length desc nulls first) as key_length_cume_dist,
        rank() over (partition by key order by key_length) as key_length_rank
    from valid
    where key_length > 62
),
filtered as (
    -- Keep recent, valid rows
    select *, round(key_length * 0.9, 2) as key_length_adjusted
    from metrics
    where element_sum_key_length <= 282
      and key is not null
)
-- Multi-level aggregation
select
    key,
    case when key_length >= 82 then 'LARGE' else 'SMALL' end as key_length_tier,
    count(*) as row_count,
    sum(case when key = 'gift' then key_length_adjusted else 0 end) as gift_key_length_adjusted,
    round(avg(key_length), 2) as avg_key_length,
    sum(element_sum_key_length) filter (where key = 'device') as device_total_element_sum_key_length,
    count(*) as row_count_2
from filtered
group by cube (key, case when key_length >= 82 then 'LARGE' else 'SMALL' end)
having sum(element_sum_key_length) filter (where key = 'device') < 1000000
order by row_count_2
