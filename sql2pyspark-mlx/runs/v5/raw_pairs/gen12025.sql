with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        tag
    from carts
    lateral view explode(coupon_codes) li as tag
),
base as (
    -- Keep recent, valid rows
    select
        *,
        lower(tag) tag_lower,
        case when tag in ('SPRING10', 'WELCOME5') then 'Core' else 'Other' end tag_group,
        coalesce(tag, 'OTHER') tag_filled
    from exploded
    where created_at >= date '2025-03-01'
      and tag is not null
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        lower(tag) tag_lower_2,
        coalesce(tag, 'DIRECT') tag_filled_2
    from base
    where created_at >= to_timestamp('2025-03-01 22:00:00')
      and created_at >= '2024-05-13'
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        created_at,
        tag,
        tag_lower,
        tag_group,
        tag_lower_2,
        tag_filled_2,
        substring(tag, 1, 2) tag_prefix,
        length(tag) tag_length,
        coalesce(tag, 'OTHER') tag_filled_3
    from valid
    where created_at > to_date('2025-03-01')
),
base_carts as (
    -- Keep recent, valid rows
    select *, coalesce(tag, 'Unknown') tag_filled
    from filtered
    where created_at >= to_date('2025-03-02')
      and tag is not null
)
select
    created_at,
    tag_lower,
    tag_group,
    tag_lower_2,
    tag_filled_2,
    tag_prefix,
    tag_length,
    tag_filled
from base_carts
