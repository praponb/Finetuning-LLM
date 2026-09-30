with exploded as (
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    from carts
    lateral view explode(attributes) t as key, value
),
flattened as (
    select
        id,
        created_at,
        key,
        value,
        explode(coupon_codes) as value_2
    from exploded
),
base as (
    select *, lower(value_2) as value_2_lower
    from flattened
    where key is not null
),
filtered as (
    select
        id,
        created_at,
        key,
        value,
        value_2,
        datediff(current_date(), created_at) as days_since_created_at,
        coalesce(value_2, 'UNKNOWN') as resolved_value_2
    from base
    where created_at >= to_date('2025-03-02')
      and hour(created_at) between 7 and 13
),
cleaned as (
    select
        *,
        upper(value_2) as value_2_upper,
        upper(value_2) as value_2_upper_2,
        coalesce(value_2, 'OTHER') as value_2_filled
    from filtered
    where created_at > date '2025-03-01'
      and value_2 is not null
),
enriched as (
    select
        *,
        dense_rank() over (partition by value_2 order by days_since_created_at desc) as days_since_created_at_dense_rank,
        sum(days_since_created_at) over (partition by key order by cast(created_at as bigint) range between 7200 preceding and current row) as days_since_created_at_sum_2h,
        min(days_since_created_at) over (partition by key) as key_min_days_since_created_at,
        sum(days_since_created_at) over (partition by key, value_2) as key_sum_days_since_created_at,
        avg(days_since_created_at) over w_grp as days_since_created_at_avg_15m
    from cleaned
    window w_grp as (partition by key order by cast(created_at as bigint) range between 900 preceding and current row)
),
enriched_carts as (
    select
        id,
        created_at,
        key,
        value,
        value_2,
        days_since_created_at,
        resolved_value_2,
        value_2_upper,
        value_2_upper_2,
        value_2_filled,
        days_since_created_at_dense_rank,
        days_since_created_at_sum_2h,
        key_min_days_since_created_at,
        key_sum_days_since_created_at,
        days_since_created_at_avg_15m,
        min(key_min_days_since_created_at) over w_part as key_min_key_min_days_since_created_at,
        percent_rank() over (partition by value_2 order by days_since_created_at_dense_rank nulls first) as days_since_created_at_dense_rank_percent_rank,
        rank() over (order by days_since_created_at desc) as days_since_created_at_rank,
        percent_rank() over w_part_2 as days_since_created_at_dense_rank_percent_rank_2,
        cume_dist() over (partition by value_2, key order by key_sum_days_since_created_at nulls first) as key_sum_days_since_created_at_cume_dist
    from enriched
    window w_part as (partition by key), w_part_2 as (partition by key order by days_since_created_at_dense_rank desc nulls first)
)
select
    id,
    key,
    value,
    value_2,
    days_since_created_at,
    value_2_upper,
    value_2_upper_2,
    value_2_filled,
    days_since_created_at_dense_rank
from enriched_carts
order by value_2 desc nulls first;
