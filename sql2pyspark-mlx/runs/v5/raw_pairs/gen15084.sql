with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        k,
        attr_value
    from carts
    lateral view explode(attributes) t as k, attr_value
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        coupon_codes,
        k,
        array_join(coupon_codes, ', ') as coupon_codes_text
    from exploded
    where array_contains(coupon_codes, 'WELCOME5')
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        coupon_codes,
        k,
        coupon_codes_text,
        length(k) as k_length,
        size(coupon_codes) as coupon_codes_count,
        coalesce(k, 'UNKNOWN') as k_filled
    from base
    where k is not null
),
base_carts as (
    -- Basic cleansing and defaults
    select
        id,
        k,
        coupon_codes_text,
        k_length,
        k_filled,
        sort_array(coupon_codes) as sorted_coupon_codes,
        coalesce(k, 'OTHER') as resolved_k
    from cleaned
    where array_contains(coupon_codes, 'VIP')
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        k,
        coupon_codes_text,
        k_length,
        k_filled,
        resolved_k,
        sort_array(sorted_coupon_codes) as sorted_sorted_coupon_codes,
        coalesce(k, 'NONE') as k_filled_2
    from base_carts
    where k_length >= 55
),
filtered as (
    -- Basic cleansing and defaults
    select *, case when k = 'gift' then 1 else 0 end as is_gift
    from valid
    where k_length >= 60
)
select
    id,
    k,
    coupon_codes_text,
    k_length
from filtered
order by k;
