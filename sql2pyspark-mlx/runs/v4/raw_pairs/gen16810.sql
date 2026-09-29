with scored as (
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        cume_dist() over (partition by product_id, store_id order by quantity nulls last) quantity_cume_dist,
        first_value(quantity, true) over (partition by product_id order by last_restock, id rows between unbounded preceding and current row) first_known_quantity,
        cume_dist() over (partition by store_id order by quantity nulls first) quantity_cume_dist_2
    from inventory
),
enriched as (
    select
        *,
        percent_rank() over (partition by product_id order by first_known_quantity desc) first_known_quantity_percent_rank,
        lag(quantity, 2) over (partition by store_id, product_id order by last_restock, id) prev_quantity,
        ntile(4) over (partition by product_id order by first_known_quantity desc, id) first_known_quantity_bucket,
        ntile(4) over (partition by product_id order by quantity_cume_dist, id) quantity_cume_dist_bucket,
        dense_rank() over (partition by product_id order by first_known_quantity desc nulls last) first_known_quantity_dense_rank
    from scored
),
valid as (
    select *, round(first_known_quantity_dense_rank * 1.1, 2) adjusted_first_known_quantity_dense_rank
    from enriched
    where last_restock > date '2024-01-19'
),
enriched_inventory as (
    select
        src.*,
        d.price,
        d.brand
    from valid src
    left join products d
        on src.product_id = d.id
),
totals as (
    select
        case when first_known_quantity >= 263 then 'HIGH' else 'LOW' end as first_known_quantity_tier,
        case when adjusted_first_known_quantity_dense_rank >= 82 then 'PREMIUM' else 'STANDARD' end as adjusted_first_known_quantity_dense_rank_tier,
        case when quantity_cume_dist_2 >= 0.6 then 'LARGE' else 'SMALL' end as quantity_cume_dist_2_tier,
        size(collect_set(brand)) as num_brands,
        approx_percentile(quantity, 0.95) as quantity_p95,
        avg(first_known_quantity_dense_rank) filter (where brand = 'Initech') as initech_avg_first_known_quantity_dense_rank
    from enriched_inventory
    group by case when first_known_quantity >= 263 then 'HIGH' else 'LOW' end, case when adjusted_first_known_quantity_dense_rank >= 82 then 'PREMIUM' else 'STANDARD' end, case when quantity_cume_dist_2 >= 0.6 then 'LARGE' else 'SMALL' end
)
select
    first_known_quantity_tier,
    adjusted_first_known_quantity_dense_rank_tier,
    quantity_cume_dist_2_tier,
    quantity_p95,
    initech_avg_first_known_quantity_dense_rank
from totals
