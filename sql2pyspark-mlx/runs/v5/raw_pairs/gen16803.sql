with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        explode_outer(coupon_codes) as value
    from carts
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        length(value) as value_length,
        upper(value) as value_upper,
        coalesce(value, 'OTHER') as value_filled
    from exploded
    where value is distinct from 'BUNDLE'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        cume_dist() over (order by value_length) as value_length_cume_dist,
        sum(value_length) over (partition by value) as value_sum_value_length,
        sum(value_length) over (partition by value) as value_sum_value_length_2,
        sum(value_length) over (partition by value) as value_sum_value_length_3,
        sum(value_length) over (partition by value order by cast(created_at as bigint) range between 3600 preceding and current row) as value_length_sum_1h
    from filtered
    where created_at < '2024-05-01'
),
with_windows_carts as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (order by value_sum_value_length_2 desc nulls first) as value_sum_value_length_2_percent_rank,
        max(value_sum_value_length_3) over (partition by value order by cast(created_at as bigint) range between 7200 preceding and current row) as value_sum_value_length_3_max_2h,
        count(value_sum_value_length_3) over (order by cast(created_at as bigint) range between 1800 preceding and current row) as value_sum_value_length_3_count_30m,
        avg(value_sum_value_length_3) over w_running as value_avg_value_sum_value_length_3
    from with_windows
    window w_running as (partition by value)
),
metrics as (
    -- Window metrics over several different partitions
    select
        created_at,
        value_length_cume_dist,
        value_sum_value_length_2,
        value_length_sum_1h,
        value_sum_value_length_2_percent_rank,
        value_sum_value_length_3_max_2h,
        value_avg_value_sum_value_length_3,
        rank() over (partition by value order by value_length_cume_dist desc nulls first) as value_length_cume_dist_rank,
        count(value_sum_value_length_3) over (partition by value order by cast(created_at as bigint) range between 1800 preceding and current row) as value_sum_value_length_3_count_30m_2,
        rank() over (partition by value order by value_sum_value_length_3_count_30m desc nulls last) as value_sum_value_length_3_count_30m_rank
    from with_windows_carts
)
-- Aggregate with conditional metrics
select
    case when value_avg_value_sum_value_length_3 >= 440 then 'HIGH' else 'LOW' end as value_avg_value_sum_value_length_3_tier,
    percentile_approx(value_sum_value_length_3_max_2h, 0.5) as value_sum_value_length_3_max_2h_p50,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as duration_min,
    approx_percentile(value_avg_value_sum_value_length_3, array(0.25, 0.5, 0.75)) as value_avg_value_sum_value_length_3_quartiles
from metrics
group by case when value_avg_value_sum_value_length_3 >= 440 then 'HIGH' else 'LOW' end
order by value_sum_value_length_3_max_2h_p50 nulls first;
