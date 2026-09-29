with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        inline(items)
    from carts
)
-- Ranking and gap features
select
    created_at,
    coupon_codes,
    product_id,
    promo,
    count(price) over (partition by customer_id) as customer_id_count_price,
    avg(qty) over (partition by promo, customer_id) as promo_avg_qty,
    sum(product_id) over (partition by promo, customer_id order by cast(created_at as bigint) range between 7200 preceding and current row) as product_id_sum_2h,
    sum(product_id) over (partition by customer_id order by cast(created_at as bigint) range between 7200 preceding and current row) as product_id_sum_2h_2
from exploded
