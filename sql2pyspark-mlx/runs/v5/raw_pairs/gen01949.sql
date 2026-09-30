with matched_keys as (
    select product_id
    from order_items
    intersect all
    select product_id
    from order_items
    where unit_price between 320 and 330
),
reconciled as (
    select s.*
    from order_items s
    join matched_keys k on s.product_id = k.product_id
),
metrics as (
    select
        id,
        order_id,
        corr(quantity, unit_price) over (partition by product_id) as corr_quantity_unit_price,
        corr(quantity, unit_price) over (partition by product_id) as corr_quantity_unit_price_2,
        round(quantity / sum(quantity) over (partition by product_id, order_id), 4) as quantity_share,
        round(unit_price / sum(unit_price) over (partition by order_id), 4) as unit_price_share,
        count(unit_price) over (partition by product_id) as product_id_count_unit_price
    from reconciled
),
base as (
    select
        id,
        corr_quantity_unit_price,
        quantity_share,
        round(corr_quantity_unit_price_2 * 1.07, 2) as corr_quantity_unit_price_2_scaled
    from metrics
    where corr_quantity_unit_price_2 >= 0.3
),
valid as (
    select
        *,
        coalesce(corr_quantity_unit_price, 0) as corr_quantity_unit_price_filled,
        corr_quantity_unit_price + corr_quantity_unit_price_2_scaled as corr_quantity_unit_price_plus_corr_quantity_unit_price_2_scaled
    from base
    where quantity_share < 0.66
),
metrics_order_items as (
    select
        *,
        rank() over (order by corr_quantity_unit_price desc nulls first) as corr_quantity_unit_price_rank,
        cume_dist() over (order by corr_quantity_unit_price_2_scaled desc nulls first) as corr_quantity_unit_price_2_scaled_cume_dist
    from valid
),
with_windows as (
    select
        *,
        dense_rank() over (order by corr_quantity_unit_price desc nulls last) as corr_quantity_unit_price_dense_rank,
        cume_dist() over (order by quantity_share desc) as quantity_share_cume_dist,
        percent_rank() over (order by corr_quantity_unit_price_2_scaled desc nulls first) as corr_quantity_unit_price_2_scaled_percent_rank,
        cume_dist() over (order by corr_quantity_unit_price) as corr_quantity_unit_price_cume_dist
    from metrics_order_items
)
select
    case when corr_quantity_unit_price_dense_rank >= 8 then 'PREMIUM' else 'STANDARD' end as corr_quantity_unit_price_dense_rank_tier,
    max(quantity_share) as max_quantity_share,
    count(*) as cnt
from with_windows
group by case when corr_quantity_unit_price_dense_rank >= 8 then 'PREMIUM' else 'STANDARD' end
