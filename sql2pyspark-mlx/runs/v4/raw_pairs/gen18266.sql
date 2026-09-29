with lines as (
    select
        id,
        coupon_codes,
        line.product_id,
        line.price
    from carts
    lateral view outer explode(items) li as line
),
metrics as (
    select
        *,
        dense_rank() over w_grp as price_dense_rank,
        dense_rank() over (order by product_id desc nulls last) as product_id_dense_rank
    from lines
    window w_grp as (order by price nulls last)
),
base as (
    select
        id,
        coupon_codes,
        product_id,
        price,
        product_id_dense_rank,
        round(price_dense_rank / price, 2) as price_dense_rank_per_price,
        case when product_id_dense_rank >= 8 then 'High' when product_id_dense_rank >= 3 then 'Medium' else 'Low' end as product_id_dense_rank_band
    from metrics
    where array_contains(coupon_codes, 'SPRING10')
),
filtered as (
    select
        id,
        coupon_codes,
        product_id,
        product_id_dense_rank,
        price_dense_rank_per_price,
        price_dense_rank_per_price * price as price_dense_rank_per_price_times_price
    from base
    where price <= 200
)
select
    id,
    product_id,
    product_id_dense_rank
from filtered
order by product_id_dense_rank desc nulls last
