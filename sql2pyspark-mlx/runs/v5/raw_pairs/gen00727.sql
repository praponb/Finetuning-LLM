with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        key,
        attr_value
    from carts
    lateral view outer explode(attributes) t as key, attr_value
),
flattened as (
    -- Unnest the array into one row per element
    select
        key,
        attr_value,
        explode(coupon_codes) as tag
    from exploded
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        lower(tag) as tag_lower,
        coalesce(key, 'UNKNOWN') as key_filled
    from flattened
    where key is not null
),
cleaned as (
    -- Basic cleansing and defaults
    select
        key,
        attr_value,
        tag,
        tag_lower,
        key_filled,
        trim(tag) as clean_tag
    from filtered
    where key = 'device'
),
base as (
    -- Keep recent, valid rows
    select
        key,
        tag_lower,
        key_filled,
        clean_tag,
        initcap(trim(tag)) as tag_title,
        concat(tag, ' - ', key) as tag_key,
        coalesce(key, 'OTHER') as key_clean
    from cleaned
    where tag is not null
      and tag = 'VIP'
)
select
    key,
    tag_lower,
    key_filled,
    tag_title,
    key_clean,
    case when key rlike '^(device|gift)$' then 'GROUP_B' else 'OTHER_ZONE' end as key_zone
from base
order by key_zone desc, key;
