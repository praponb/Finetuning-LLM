with exploded as (
    select
        id,
        coupon_codes,
        item.product_id,
        item.qty,
        item.price
    from carts
    lateral view explode(items) e as item
),
enriched as (
    select
        id,
        coupon_codes,
        product_id,
        qty,
        price,
        cume_dist() over (order by price desc) as price_cume_dist,
        rank() over (order by qty asc) as qty_rank,
        dense_rank() over (order by price asc) as price_dense_rank,
        cume_dist() over (order by price desc) as price_cume_dist_2
    from exploded
),
kept_keys as (
    select id
    from enriched
    except
    select id
    from carts
    where customer_id = 35
),
reconciled as (
    select s.*
    from enriched s
    join kept_keys k on s.id = k.id
),
enriched_carts as (
    select
        *,
        dense_rank() over (order by price desc) as price_dense_rank_2,
        rank() over (order by qty_rank asc) as qty_rank_rank,
        dense_rank() over (order by qty_rank desc nulls first) as qty_rank_dense_rank
    from reconciled
),
enriched_carts_2 as (
    select
        id,
        coupon_codes,
        product_id,
        qty,
        price,
        price_cume_dist,
        qty_rank,
        price_dense_rank,
        price_cume_dist_2,
        price_dense_rank_2,
        qty_rank_rank,
        qty_rank_dense_rank,
        percent_rank() over (order by qty_rank_dense_rank asc nulls last) as qty_rank_dense_rank_percent_rank,
        percent_rank() over (order by price_dense_rank desc) as price_dense_rank_percent_rank,
        dense_rank() over (order by qty_rank desc) as qty_rank_dense_rank_2
    from enriched_carts
),
scored as (
    select
        *,
        percent_rank() over (order by price_cume_dist_2 desc nulls last) as price_cume_dist_2_percent_rank,
        dense_rank() over (order by price_cume_dist asc nulls last) as price_cume_dist_dense_rank,
        rank() over w_running as price_cume_dist_2_rank,
        percent_rank() over (order by price_cume_dist desc) as price_cume_dist_percent_rank
    from enriched_carts_2
    window w_running as (order by price_cume_dist_2 asc)
),
with_windows as (
    select
        *,
        dense_rank() over (order by price desc nulls last) as price_dense_rank_3,
        percent_rank() over (order by price_cume_dist_2_rank desc nulls last) as price_cume_dist_2_rank_percent_rank
    from scored
)
select
    *,
    percent_rank() over (order by price_cume_dist_2_rank asc nulls last) as price_cume_dist_2_rank_percent_rank_2,
    percent_rank() over (order by price_cume_dist_dense_rank asc) as price_cume_dist_dense_rank_percent_rank,
    rank() over (order by price_cume_dist_2_percent_rank asc) as price_cume_dist_2_percent_rank_rank
from with_windows;
