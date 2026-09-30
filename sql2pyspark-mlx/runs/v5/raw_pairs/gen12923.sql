with flattened as (
    select
        id,
        attr_key,
        value
    from carts
    lateral view explode(attributes) t as attr_key, value
),
base as (
    select *, initcap(trim(attr_key)) as attr_key_title
    from flattened
    where attr_key is not null
),
base_carts as (
    select
        *,
        initcap(trim(attr_key)) as attr_key_title_2,
        case when attr_key = 'utm_source' then 1 else 0 end as is_utm_source,
        coalesce(attr_key, 'OTHER') as resolved_attr_key
    from base
    where attr_key is not distinct from 'gift'
),
base_carts_2 as (
    select
        id,
        attr_key,
        attr_key_title,
        attr_key_title_2,
        is_utm_source,
        resolved_attr_key,
        floor(is_utm_source) as is_utm_source_whole,
        round(is_utm_source * 100, 2) as adjusted_is_utm_source
    from base_carts
    where is_utm_source <= 30
)
select
    case when adjusted_is_utm_source >= 76 then 'PREMIUM' else 'STANDARD' end as adjusted_is_utm_source_tier,
    attr_key,
    count(*) filter (where attr_key = 'utm_source') as utm_source_rows,
    count(*) filter (where attr_key = 'gift') as gift_rows,
    count(*) filter (where attr_key = 'device') as device_rows,
    round(avg(adjusted_is_utm_source), 2) as avg_adjusted_is_utm_source,
    percent_rank() over (partition by case when adjusted_is_utm_source >= 76 then 'PREMIUM' else 'STANDARD' end order by count(*) filter (where attr_key = 'device') desc) as device_rows_percent_rank
from base_carts_2
group by case when adjusted_is_utm_source >= 76 then 'PREMIUM' else 'STANDARD' end, attr_key
