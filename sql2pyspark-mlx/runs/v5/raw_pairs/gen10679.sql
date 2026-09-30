with lines as (
    select
        id,
        coupon_codes,
        key,
        value
    from carts
    lateral view explode(attributes) t as key, value
),
base as (
    select
        *,
        sort_array(coupon_codes) as sorted_coupon_codes,
        coalesce(key, 'DIRECT') as key_clean
    from lines
    where array_contains(coupon_codes, 'VIP')
),
filtered as (
    select
        id,
        coupon_codes,
        key,
        value,
        sorted_coupon_codes,
        size(coupon_codes) as coupon_codes_count
    from base
    where key is not null
),
cleaned as (
    select
        *,
        round(coupon_codes_count, 1) as coupon_codes_count_rounded,
        coalesce(key, 'N/A') as resolved_key
    from filtered
    where key is not null
      and key is distinct from 'utm_source'
),
ranked as (
    select
        id,
        key,
        coupon_codes_count,
        coupon_codes_count_rounded
    from (
        select *, rank() over (partition by key order by coupon_codes_count_rounded) as dedup_rank
        from cleaned
    ) t
    where dedup_rank = 1
)
select
    *,
    first_value(coupon_codes_count_rounded) ignore nulls over (order by coupon_codes_count_rounded, id, key rows between unbounded preceding and current row) as first_known_coupon_codes_count_rounded,
    ntile(10) over (partition by key order by coupon_codes_count, id, key) as coupon_codes_count_bucket
from ranked
where coupon_codes_count <= 58;
