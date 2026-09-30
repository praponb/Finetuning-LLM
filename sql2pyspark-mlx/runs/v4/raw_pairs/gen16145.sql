with lines as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        li_item.qty,
        li_item.price,
        li_item.promo
    from carts
    lateral view explode(items) li as li_item
),
metrics as (
    -- Ranking and gap features
    select
        id,
        created_at,
        coupon_codes,
        qty,
        price,
        promo,
        sum(price) over (partition by promo) as promo_sum_price,
        round(qty / sum(qty) over (partition by promo), 4) as qty_share
    from lines
),
base as (
    -- Keep recent, valid rows
    select
        *,
        case when promo_sum_price >= 908 then 'High' when promo_sum_price >= 173 then 'Medium' else 'Low' end as promo_sum_price_level,
        greatest(qty_share, qty) as greatest_qty_share_qty,
        coalesce(promo, 'NONE') as promo_filled
    from metrics
    where created_at >= date '2025-03-02'
      and promo is not null
      and hour(created_at) between 10 and 13
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        promo,
        price,
        greatest_qty_share_qty,
        qty_share
    from base
) src
unpivot (
    value for metric_name in (price, greatest_qty_share_qty, qty_share)
);
