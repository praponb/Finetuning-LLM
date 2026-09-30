with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    from carts
    lateral view outer explode(attributes) t as key, value
),
lines_carts as (
    -- Unnest the array into one row per element
    select
        key,
        value,
        value_2
    from lines
    lateral view outer explode(coupon_codes) li as value_2
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, case when value_2 = 'WELCOME5' then 1 else 0 end as is_welcome5
    from lines_carts
    where key in ('device', 'utm_source')
),
valid as (
    -- Basic cleansing and defaults
    select *, initcap(trim(key)) as key_title
    from cleaned
    where value_2 is not null
),
valid_carts as (
    -- Basic cleansing and defaults
    select
        *,
        initcap(trim(value_2)) as value_2_title,
        coalesce(is_welcome5, 0) as is_welcome5_filled,
        coalesce(value_2, 'NONE') as resolved_value_2
    from valid
    where value_2 is not null
      and key is distinct from 'utm_source'
),
metrics as (
    -- Ranking and gap features
    select
        *,
        stddev_samp(is_welcome5_filled) over w_part as key_stddev_samp_is_welcome5_filled,
        cume_dist() over w_grp as is_welcome5_filled_cume_dist,
        min(is_welcome5_filled) over w_part as key_min_is_welcome5_filled,
        cume_dist() over (partition by key, value_2 order by is_welcome5 desc nulls first) as is_welcome5_cume_dist,
        rank() over w_recent as is_welcome5_rank
    from valid_carts
    window w_part as (partition by key), w_grp as (partition by value_2 order by is_welcome5_filled nulls first), w_recent as (partition by key, value_2 order by is_welcome5)
),
valid_carts_2 as (
    -- Basic cleansing and defaults
    select
        key,
        value,
        value_2,
        is_welcome5,
        key_title,
        value_2_title,
        is_welcome5_filled,
        resolved_value_2,
        key_stddev_samp_is_welcome5_filled,
        is_welcome5_filled_cume_dist,
        is_welcome5_cume_dist,
        is_welcome5_rank,
        is_welcome5_filled - is_welcome5_cume_dist as is_welcome5_filled_minus_is_welcome5_cume_dist
    from metrics
    where key is not distinct from 'gift'
)
-- Grouped metrics
select
    value_2,
    sort_array(collect_set(key)) as key_list,
    count(case when key = 'utm_source' then 1 end) as utm_source_count
from valid_carts_2
group by 1;
