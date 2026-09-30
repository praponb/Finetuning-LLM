with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        elem.qty,
        elem.price,
        elem.promo
    from carts
    lateral view outer explode(items) x as elem
),
exploded as (
    -- Unnest the array into one row per element
    select
        qty,
        promo,
        explode(coupon_codes) as element
    from lines
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, concat(element, ' ', promo) as element_promo
    from exploded
    where promo != 'BUNDLE'
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        concat_ws('/', element, promo) as element_promo_key,
        coalesce(element, 'N/A') as element_filled
    from base
    where element = 'SPRING10'
),
valid_carts as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when element = 'BUNDLE' then 1 else 0 end as is_bundle,
        coalesce(promo, 'DIRECT') as promo_clean
    from valid
    where element is not null
      and element is distinct from 'BUNDLE'
)
select
    qty,
    promo,
    element_promo_key,
    element_filled,
    is_bundle,
    case when promo rlike '^(SPRING10|VIP)$' then 'OTHER_ZONE' else 'GROUP_A' end as promo_zone
from valid_carts
order by qty desc;
