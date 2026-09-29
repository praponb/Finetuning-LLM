with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        line.product_id,
        line.price,
        line.promo
    from carts
    lateral view explode(items) e as line
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        hour(created_at) created_at_hour,
        coalesce(promo, 'NONE') promo_filled
    from flattened
    where created_at >= date '2025-03-02'
      and promo is not null
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        created_at,
        price,
        promo,
        created_at_hour,
        promo_filled,
        created_at_hour * product_id created_at_hour_times_product_id,
        coalesce(promo, 'OTHER') promo_filled_2
    from base
    where created_at > timestamp '2025-03-01 13:00:00'
      and promo is not null
),
scored as (
    -- Ranking and gap features
    select
        created_at,
        price,
        promo,
        created_at_hour,
        promo_filled,
        created_at_hour_times_product_id,
        max(created_at_hour_times_product_id) over (partition by promo) promo_max_created_at_hour_times_product_id,
        stddev_samp(created_at_hour) over (partition by promo) promo_stddev_samp_created_at_hour
    from valid
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        case when promo_stddev_samp_created_at_hour >= 77 then 'High' when promo_stddev_samp_created_at_hour >= 36 then 'Medium' else 'Low' end promo_stddev_samp_created_at_hour_level,
        coalesce(promo, 'DIRECT') resolved_promo
    from scored
    where promo is not distinct from 'BUNDLE'
      and promo is not null
)
select
    created_at,
    created_at_hour_times_product_id,
    promo_max_created_at_hour_times_product_id,
    promo_stddev_samp_created_at_hour_level
from cleaned
