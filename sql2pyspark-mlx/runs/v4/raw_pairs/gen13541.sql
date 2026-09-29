with lines as (
    -- Unnest the array into one row per element
    select
        id,
        coupon_codes,
        li_item.price,
        li_item.promo
    from carts
    lateral view outer explode(items) li as li_item
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        round(price / sum(price) over (partition by promo), 4) as price_share,
        percent_rank() over (partition by promo order by price desc) as price_percent_rank
    from lines
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        coupon_codes,
        price,
        price_percent_rank,
        array_contains(coupon_codes, 'WELCOME5') as is_welcome5,
        size(coupon_codes) as coupon_codes_count,
        coalesce(promo, 'N/A') as promo_clean
    from with_windows
    where promo is not null
      and price_percent_rank < 0.34
),
with_windows_carts as (
    -- Ranking and gap features
    select
        *,
        rank() over (order by price desc) as price_rank,
        percent_rank() over (order by coupon_codes_count desc nulls first) as coupon_codes_count_percent_rank
    from filtered
)
select
    is_welcome5,
    coupon_codes_count,
    price_rank,
    coupon_codes_count_percent_rank
from with_windows_carts
order by coupon_codes_count_percent_rank desc
