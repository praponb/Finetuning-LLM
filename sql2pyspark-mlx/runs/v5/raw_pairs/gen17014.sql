with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        inline(items)
    from carts
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        qty,
        promo,
        stddev_samp(price) over (partition by customer_id) as customer_id_stddev_samp_price,
        rank() over (order by product_id desc nulls first) as product_id_rank,
        max(product_id) over (partition by promo) as promo_max_product_id,
        count(product_id) over (partition by promo) as promo_count_product_id,
        rank() over (partition by promo, customer_id order by product_id nulls last) as product_id_rank_2
    from lines
),
metrics as (
    -- Ranking and gap features
    select
        id,
        customer_id_stddev_samp_price,
        promo_count_product_id,
        dense_rank() over w_running as product_id_rank_2_dense_rank,
        round(product_id_rank_2 / sum(product_id_rank_2) over (partition by customer_id), 4) as product_id_rank_2_share,
        count(product_id_rank_2) over w_ordered as customer_id_count_product_id_rank_2
    from with_windows
    window w_running as (partition by customer_id order by product_id_rank_2 desc nulls first), w_ordered as (partition by customer_id)
),
with_windows_carts as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (order by product_id_rank_2_dense_rank nulls last) as product_id_rank_2_dense_rank_percent_rank,
        cume_dist() over (order by product_id_rank_2_dense_rank desc nulls last) as product_id_rank_2_dense_rank_cume_dist
    from metrics
    where customer_id_stddev_samp_price is not null
)
select
    id,
    customer_id_stddev_samp_price,
    promo_count_product_id,
    product_id_rank_2_dense_rank,
    product_id_rank_2_share,
    customer_id_count_product_id_rank_2,
    product_id_rank_2_dense_rank_percent_rank,
    product_id_rank_2_dense_rank_cume_dist
from with_windows_carts
order by customer_id_count_product_id_rank_2 desc nulls last;
