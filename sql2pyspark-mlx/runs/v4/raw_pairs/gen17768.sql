with flattened as (
    select
        id,
        created_at,
        coupon_codes,
        k,
        v
    from carts
    lateral view outer explode(attributes) e as k, v
),
filtered as (
    select
        *,
        array_contains(coupon_codes, 'BUNDLE') as is_bundle,
        weekofyear(created_at) as created_at_weekofyear,
        coalesce(k, 'NONE') as k_filled
    from flattened
    where k is not null
),
base as (
    select
        *,
        floor(created_at_weekofyear) as created_at_weekofyear_whole,
        coalesce(k, 'OTHER') as k_clean
    from filtered
    where k is not null
      and is_bundle
),
scored as (
    select
        id,
        created_at,
        coupon_codes,
        k,
        v,
        is_bundle,
        created_at_weekofyear,
        k_filled,
        created_at_weekofyear_whole,
        k_clean,
        sum(created_at_weekofyear) over (partition by k order by cast(created_at as bigint) asc range between 7200 preceding and current row) as created_at_weekofyear_sum_2h,
        rank() over (partition by k order by created_at_weekofyear_whole asc nulls first) as created_at_weekofyear_whole_rank
    from base
),
metrics as (
    select
        id,
        created_at,
        coupon_codes,
        k,
        v,
        is_bundle,
        created_at_weekofyear,
        k_filled,
        created_at_weekofyear_whole,
        k_clean,
        created_at_weekofyear_sum_2h,
        created_at_weekofyear_whole_rank,
        max(created_at_weekofyear_whole) over (partition by k) as k_max_created_at_weekofyear_whole,
        avg(created_at_weekofyear) over (partition by k order by cast(created_at as bigint) asc range between 3600 preceding and current row) as created_at_weekofyear_avg_1h,
        count(created_at_weekofyear_whole) over (partition by k order by cast(created_at as bigint) asc range between 7200 preceding and current row) as created_at_weekofyear_whole_count_2h
    from scored
),
report as (
    select
        k,
        v,
        is_bundle,
        created_at_weekofyear,
        created_at_weekofyear_whole,
        created_at_weekofyear_sum_2h,
        created_at_weekofyear_whole_rank,
        k_max_created_at_weekofyear_whole,
        created_at_weekofyear_avg_1h,
        case when k rlike '^(utm_source|device)$' then 'OTHER_ZONE' else 'PRIORITY' end as k_zone
    from metrics
    order by created_at_weekofyear_whole desc nulls last
)
select
    is_bundle,
    created_at_weekofyear_whole,
    created_at_weekofyear_sum_2h,
    k_max_created_at_weekofyear_whole,
    created_at_weekofyear_avg_1h
from report
order by created_at_weekofyear_whole asc, k_max_created_at_weekofyear_whole asc;
