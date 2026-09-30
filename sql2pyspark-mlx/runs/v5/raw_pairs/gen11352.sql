with lines as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        key,
        value
    from carts
    lateral view outer explode(attributes) e as key, value
),
flattened as (
    -- Unnest the array into one row per element
    select value, explode(coupon_codes) as tag
    from lines
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when tag in ('VIP', 'SPRING10') then 'Group A' else 'Other' end as tag_group,
        upper(tag) as tag_upper
    from flattened
    where tag is not null
      and tag = 'BUNDLE'
),
base as (
    -- Basic cleansing and defaults
    select *, substring(tag, 1, 4) as tag_prefix
    from valid
    where tag is not null
      and tag is not null
)
-- Aggregate with conditional metrics
select
    tag,
    count_if(tag = 'WELCOME5') as welcome5_events,
    size(collect_set(tag)) as num_tags,
    sort_array(collect_set(tag)) as tag_list,
    grouping(tag) as grouping_level
from base
group by cube (tag)
order by welcome5_events nulls last;
