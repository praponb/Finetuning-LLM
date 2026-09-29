with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    from (
        select
            id,
            customer_id,
            created_at,
            coupon_codes,
            explode(items) as li_item
        from carts
    ) t
),
valid as (
    -- Keep recent, valid rows
    select *, ceil(product_id) as product_id_whole
    from exploded
    where created_at >= timestamp '2025-03-02 00:00:00'
),
metrics as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by customer_id, promo order by qty nulls first) as qty_percent_rank,
        rank() over (order by qty desc nulls first) as qty_rank,
        cume_dist() over w_ordered as product_id_whole_cume_dist,
        avg(product_id) over w_hist as product_id_avg_30m,
        count(price) over (partition by promo order by cast(created_at as bigint) range between 1800 preceding and current row) as price_count_30m
    from valid
    window w_ordered as (order by product_id_whole), w_hist as (partition by customer_id order by cast(created_at as bigint) range between 1800 preceding and current row)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        price * product_id_whole as price_times_product_id_whole,
        array_join(coupon_codes, '|') as coupon_codes_text
    from metrics
    where promo = 'SPRING10'
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (partition by customer_id order by price_count_30m desc) as price_count_30m_cume_dist,
        sum(price_count_30m) over (partition by promo, customer_id) as promo_sum_price_count_30m,
        dense_rank() over (order by product_id_whole nulls last) as product_id_whole_dense_rank
    from cleaned
),
summary as (
    -- Grouped metrics
    select
        case when price >= 229 then 'HIGH' else 'LOW' end as price_tier,
        round(sum(product_id_whole_dense_rank) / nullif(count(case when promo = 'VIP' then 1 end), 0), 2) as product_id_whole_dense_rank_per_vip,
        approx_percentile(price, 0.5) as price_p50,
        percentile_approx(product_id_whole_dense_rank, array(0.25, 0.5, 0.75)) as product_id_whole_dense_rank_quartiles,
        percentile_approx(qty, array(0.25, 0.5, 0.75)) as qty_quartiles,
        min(created_at) as first_created_at,
        size(collect_set(promo)) as num_promos
    from enriched
    group by case when price >= 229 then 'HIGH' else 'LOW' end
)
select
    price_tier,
    product_id_whole_dense_rank_per_vip,
    price_p50,
    product_id_whole_dense_rank_quartiles,
    qty_quartiles,
    first_created_at,
    num_promos
from summary
order by num_promos desc
