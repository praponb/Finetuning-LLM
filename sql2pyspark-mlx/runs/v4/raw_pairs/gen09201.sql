with lines as (
    -- Unnest the array into one row per element
    select
        id,
        attr_key,
        attr_value
    from carts
    lateral view outer explode(attributes) t as attr_key, attr_value
),
valid as (
    -- Keep recent, valid rows
    select *, initcap(trim(attr_key)) as attr_key_title
    from lines
    where attr_key in ('gift', 'device')
),
base as (
    -- Basic cleansing and defaults
    select *, lower(attr_key) as attr_key_lower
    from valid
    where attr_key is not null
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        upper(attr_key) as attr_key_upper,
        coalesce(attr_key, 'NONE') as attr_key_clean
    from base
    where attr_key not in ('gift', 'weight_kg', 'device')
),
filtered_carts as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        trim(attr_key) as clean_attr_key,
        coalesce(attr_key, 'NONE') as attr_key_clean_2
    from filtered
    where attr_key = 'gift'
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        initcap(trim(attr_key)) as attr_key_title_2,
        coalesce(attr_key, 'N/A') as attr_key_filled
    from filtered_carts
    where attr_key = 'gift'
)
select
    attr_key,
    attr_key_lower,
    attr_key_clean,
    clean_attr_key,
    attr_key_title_2,
    case when attr_key rlike '^(device|weight_kg)$' then 'CORE' when attr_key rlike '^(utm_source|gift)$' then 'OTHER_ZONE' else 'PRIORITY' end as attr_key_zone
from cleaned;
