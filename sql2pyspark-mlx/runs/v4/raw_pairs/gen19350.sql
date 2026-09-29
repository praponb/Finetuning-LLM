with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        v
    from carts
    lateral view outer explode(attributes) t as attr_key, v
),
flattened_carts as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        v,
        value
    from flattened
    lateral view outer explode(coupon_codes) x as value
),
base as (
    -- Keep recent, valid rows
    select
        *,
        quarter(created_at) as created_at_quarter,
        coalesce(value, 'DIRECT') as resolved_value
    from flattened_carts
    where created_at >= date '2025-03-02'
      and value is not null
      and hour(created_at) between 9 and 14
),
scored as (
    -- Running and moving aggregates
    select
        id,
        created_at,
        v,
        value,
        created_at_quarter,
        resolved_value,
        cume_dist() over (partition by value order by created_at_quarter nulls last) as created_at_quarter_cume_dist,
        cume_dist() over (partition by value order by created_at_quarter desc nulls first) as created_at_quarter_cume_dist_2
    from base
),
filtered as (
    -- Keep recent, valid rows
    select
        created_at,
        resolved_value,
        created_at_quarter_cume_dist_2,
        length(value) as value_length,
        created_at_quarter_cume_dist_2 + created_at_quarter as created_at_quarter_cume_dist_2_plus_created_at_quarter
    from scored
    where value is not null
)
select value_length, created_at_quarter_cume_dist_2_plus_created_at_quarter
from filtered
order by created_at_quarter_cume_dist_2_plus_created_at_quarter nulls first
