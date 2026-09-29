with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    from carts
    lateral view outer explode(items) x as elem
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        product_id * price product_id_times_price,
        coalesce(promo, 'N/A') promo_filled
    from flattened
    where promo is distinct from 'SPRING10'
      and price <= 150
),
scored as (
    -- Running and moving aggregates
    select
        id,
        qty,
        price,
        promo,
        round(price / sum(price) over (partition by promo), 4) price_share,
        stddev_samp(product_id) over (partition by promo) promo_stddev_samp_product_id,
        dense_rank() over (order by product_id_times_price asc nulls first) product_id_times_price_dense_rank,
        dense_rank() over (order by price desc nulls first) price_dense_rank
    from filtered
    where product_id <= 9
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        price,
        promo,
        promo_stddev_samp_product_id,
        product_id_times_price_dense_rank,
        price_dense_rank,
        price_share - qty price_share_minus_qty,
        coalesce(promo, 'OTHER') promo_filled
    from scored
    where promo is not null
),
valid_carts as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        length(promo) promo_length,
        coalesce(promo, 'NONE') promo_clean
    from valid
    where promo is not null
)
select
    id,
    price,
    promo,
    product_id_times_price_dense_rank,
    promo_filled,
    promo_clean
from valid_carts;
