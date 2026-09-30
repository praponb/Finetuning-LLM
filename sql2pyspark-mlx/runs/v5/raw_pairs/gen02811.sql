with flattened as (
    select
        id,
        customer_id,
        coupon_codes,
        attr_key,
        value
    from carts
    lateral view outer explode(attributes) t as attr_key, value
),
exploded as (
    select
        attr_key,
        value,
        code
    from flattened
    lateral view explode(coupon_codes) t as code
),
valid as (
    select
        *,
        case when attr_key = 'utm_source' then 1 else 0 end as is_utm_source,
        initcap(trim(attr_key)) as attr_key_title
    from exploded
    where attr_key is not null
      and code = 'VIP'
),
base as (
    select
        attr_key,
        value,
        code,
        is_utm_source,
        attr_key_title,
        lower(code) as code_lower,
        coalesce(code, 'OTHER') as resolved_code
    from valid
    where attr_key is not null
),
filtered as (
    select
        *,
        concat(code, ', ', attr_key) as code_attr_key,
        cast(is_utm_source as double) as is_utm_source_num,
        coalesce(attr_key, 'NONE') as attr_key_filled
    from base
    where code is not null
      and attr_key != 'device'
),
scored as (
    select
        attr_key,
        is_utm_source,
        is_utm_source_num,
        percent_rank() over w_part as is_utm_source_num_percent_rank
    from filtered
    window w_part as (order by is_utm_source_num desc)
),
valid_carts as (
    select
        *,
        greatest(is_utm_source_num_percent_rank, is_utm_source_num) as greatest_is_utm_source_num_percent_rank_is_utm_source_num,
        cast(is_utm_source_num_percent_rank as int) as is_utm_source_num_percent_rank_int
    from scored
    where attr_key != 'device'
)
select
    attr_key,
    count(distinct attr_key) as distinct_attr_keys,
    max(is_utm_source_num) as max_is_utm_source_num,
    sum(greatest_is_utm_source_num_percent_rank_is_utm_source_num) as total_greatest_is_utm_source_num_percent_rank_is_utm_source_num,
    count(*) filter (where attr_key = 'gift') as gift_rows,
    max(is_utm_source_num_percent_rank_int) filter (where attr_key = 'gift') as gift_max_is_utm_source_num_percent_rank_int
from valid_carts
group by attr_key
having gift_rows >= 1
