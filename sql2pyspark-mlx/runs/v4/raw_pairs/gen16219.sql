with exploded as (
    select
        id,
        customer_id,
        coupon_codes,
        pos,
        li_item.product_id,
        li_item.price,
        li_item.promo
    from carts
    lateral view posexplode(items) x as pos, li_item
),
matched_keys as (
    select id
    from exploded
    intersect
    select id
    from carts
    where customer_id is not null
),
kept_rows as (
    select s.*
    from exploded s
    inner join matched_keys k on s.id = k.id
),
metrics as (
    select
        *,
        avg(price) over (partition by promo) as promo_avg_price,
        round(price / sum(price) over (partition by customer_id), 4) as price_share
    from kept_rows
),
enriched as (
    select
        id,
        customer_id,
        coupon_codes,
        pos,
        product_id,
        price,
        promo,
        promo_avg_price,
        price_share,
        dense_rank() over (order by product_id nulls first) as product_id_dense_rank,
        count(price_share) over w_hist as promo_count_price_share,
        cume_dist() over (partition by promo order by price desc nulls first) as price_cume_dist
    from metrics
    window w_hist as (partition by promo)
),
cleaned as (
    select
        id,
        coupon_codes,
        pos,
        product_id,
        promo,
        promo_avg_price,
        price_share,
        product_id_dense_rank,
        promo_count_price_share,
        price_cume_dist,
        round(pos, 2) as pos_rounded
    from enriched
    where promo is not null
      and customer_id in (1, 28)
),
with_windows as (
    select
        *,
        percent_rank() over (partition by promo order by price_cume_dist desc nulls first) as price_cume_dist_percent_rank,
        avg(price_share) over w_grp as promo_avg_price_share,
        round(product_id_dense_rank / sum(product_id_dense_rank) over (partition by promo), 4) as product_id_dense_rank_share,
        avg(product_id_dense_rank) over w_grp as promo_avg_product_id_dense_rank
    from cleaned
    window w_grp as (partition by promo)
)
select
    id,
    coupon_codes,
    pos,
    product_id,
    promo,
    price_share,
    promo_count_price_share,
    pos_rounded,
    promo_avg_price_share
from with_windows
order by promo_count_price_share desc
