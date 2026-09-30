with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        attr_key,
        v
    from carts
    lateral view outer explode(attributes) e as attr_key, v
),
flattened_carts as (
    -- Unnest the array into one row per element
    select
        created_at,
        v,
        explode(coupon_codes) as element
    from flattened
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        case when element in ('BUNDLE', 'VIP') then 'Tier 1' else 'Other' end as element_group,
        hour(created_at) as created_at_hour,
        coalesce(element, 'N/A') as element_filled
    from flattened_carts
    where element is not distinct from 'SPRING10'
),
base_carts as (
    -- Filter the raw rows and derive helper columns
    select *, initcap(trim(element)) as element_title
    from base
    where created_at > date '2025-03-01'
      and element is not null
),
with_windows as (
    -- Window metrics over several different partitions
    select *, percent_rank() over (partition by element order by created_at_hour nulls last) as created_at_hour_percent_rank
    from base_carts
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        min(created_at_hour) over (partition by element) as element_min_created_at_hour,
        percent_rank() over (partition by element order by created_at_hour_percent_rank desc nulls last) as created_at_hour_percent_rank_percent_rank,
        dense_rank() over (partition by element order by created_at_hour desc nulls first) as created_at_hour_dense_rank,
        rank() over (order by created_at_hour_percent_rank desc) as created_at_hour_percent_rank_rank,
        sum(created_at_hour) over (order by cast(created_at as bigint) range between 900 preceding and current row) as created_at_hour_sum_15m
    from with_windows
)
select
    created_at,
    v,
    element_group,
    element_filled,
    element_title,
    created_at_hour_percent_rank,
    element_min_created_at_hour,
    created_at_hour_percent_rank_percent_rank,
    created_at_hour_dense_rank
from metrics
order by created_at_hour_percent_rank_percent_rank desc
