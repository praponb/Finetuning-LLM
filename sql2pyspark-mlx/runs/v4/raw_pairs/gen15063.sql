with lines as (
    -- Unnest the array into one row per element
    select id, value
    from carts
    lateral view outer explode(coupon_codes) e as value
),
filtered as (
    -- Basic cleansing and defaults
    select *, length(value) as value_length
    from lines
    where value is not null
),
scored as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (partition by value order by value_length) as value_length_percent_rank,
        percent_rank() over w_part as value_length_percent_rank_2,
        max(value_length) over w_hist as value_max_value_length,
        avg(value_length) over w_hist as value_avg_value_length
    from filtered
    window w_part as (partition by value order by value_length nulls last), w_hist as (partition by value)
),
base as (
    -- Keep recent, valid rows
    select *, round(value_avg_value_length * 0.85, 2) as value_avg_value_length_scaled
    from scored
    where value_length_percent_rank > 0.47
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        max(value_avg_value_length_scaled) over w_grp as value_max_value_avg_value_length_scaled,
        max(value_max_value_length) over w_grp as value_max_value_max_value_length
    from base
    window w_grp as (partition by value)
),
valid as (
    -- Keep recent, valid rows
    select
        value,
        value_length,
        value_max_value_length,
        value_avg_value_length,
        value_max_value_avg_value_length_scaled,
        value_max_value_max_value_length,
        value_length_percent_rank - value_avg_value_length_scaled as value_length_percent_rank_minus_value_avg_value_length_scaled
    from with_windows
    where value_length_percent_rank between 0.32 and 0.64
),
enriched as (
    -- Running and moving aggregates
    select
        value,
        value_length,
        value_max_value_length,
        value_avg_value_length,
        value_max_value_avg_value_length_scaled,
        value_max_value_max_value_length,
        value_length_percent_rank_minus_value_avg_value_length_scaled,
        dense_rank() over (partition by value order by value_max_value_length nulls last) as value_max_value_length_dense_rank,
        cume_dist() over (partition by value order by value_max_value_max_value_length nulls first) as value_max_value_max_value_length_cume_dist,
        dense_rank() over (partition by value order by value_max_value_avg_value_length_scaled nulls first) as value_max_value_avg_value_length_scaled_dense_rank,
        min(value_max_value_max_value_length) over (partition by value) as value_min_value_max_value_max_value_length
    from valid
)
select
    value,
    value_length,
    value_max_value_length,
    value_max_value_avg_value_length_scaled,
    value_max_value_max_value_length_cume_dist,
    value_max_value_avg_value_length_scaled_dense_rank,
    value_min_value_max_value_max_value_length,
    case when value rlike '^(WELCOME5|SPRING10)$' then 'GROUP_A' else 'CORE' end as value_zone
from enriched
order by value_length
