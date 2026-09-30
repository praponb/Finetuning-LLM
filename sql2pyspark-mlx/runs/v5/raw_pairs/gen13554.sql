with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        attr_value
    from carts
    lateral view explode(attributes) x as k, attr_value
),
lines as (
    -- Unnest the array into one row per element
    select
        id,
        k,
        attr_value,
        value
    from flattened
    lateral view outer explode(coupon_codes) t as value
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        length(value) as value_length,
        coalesce(value, 'OTHER') as value_clean
    from lines
    where value is distinct from 'VIP'
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        k,
        attr_value,
        value_length,
        value_clean,
        stddev_samp(value_length) over (partition by value) as value_stddev_samp_value_length
    from cleaned
    where value = 'WELCOME5'
),
stats as (
    -- Grouped metrics
    select
        k,
        case when value_stddev_samp_value_length >= 60 then 'PREMIUM' else 'STANDARD' end as value_stddev_samp_value_length_tier,
        count(case when k = 'device' then 1 end) as device_count,
        sum(value_length) filter (where k = 'gift') as gift_total_value_length,
        round(avg(value_length), 2) as avg_value_length,
        grouping(k) as grouping_id
    from with_windows
    group by grouping sets ((k, case when value_stddev_samp_value_length >= 60 then 'PREMIUM' else 'STANDARD' end), (k), ())
)
select
    k,
    value_stddev_samp_value_length_tier,
    device_count,
    avg_value_length,
    grouping_id
from stats
order by avg_value_length desc;
