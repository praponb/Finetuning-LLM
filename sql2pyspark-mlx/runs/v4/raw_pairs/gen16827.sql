with cleaned as (
    -- Keep recent, valid rows
    select *, weekofyear(last_restock) as last_restock_weekofyear
    from inventory
    where store_id in (3, 4, 6)
),
kept_keys as (
    -- Set difference on keys
    select store_id, id
    from cleaned
    except all
    select store_id, id
    from inventory
    where quantity <= 106
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join kept_keys k on s.store_id = k.store_id and s.id = k.id
),
enriched as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_weekofyear,
        avg(last_restock_weekofyear) over (partition by product_id) as product_id_avg_last_restock_weekofyear,
        dense_rank() over (order by last_restock_weekofyear desc) as last_restock_weekofyear_dense_rank,
        min(quantity) over (partition by store_id) as store_id_min_quantity
    from verified
),
summary as (
    -- Multi-level aggregation
    select
        case when store_id_min_quantity >= 250 then 'LARGE' else 'SMALL' end as store_id_min_quantity_tier,
        store_id,
        percentile_approx(store_id_min_quantity, array(0.25, 0.5, 0.75)) as store_id_min_quantity_quartiles,
        count(*) as row_count,
        round(avg(last_restock_weekofyear_dense_rank), 2) as avg_last_restock_weekofyear_dense_rank,
        approx_percentile(product_id_avg_last_restock_weekofyear, 0.9) as product_id_avg_last_restock_weekofyear_p90,
        avg(product_id_avg_last_restock_weekofyear) as avg_product_id_avg_last_restock_weekofyear,
        rank() over (partition by case when store_id_min_quantity >= 250 then 'LARGE' else 'SMALL' end order by count(*) desc) as row_count_rank
    from enriched
    group by 1, 2
    having approx_percentile(product_id_avg_last_restock_weekofyear, 0.9) < 0
)
select
    s.store_id_min_quantity_tier,
    s.store_id,
    s.row_count,
    s.avg_last_restock_weekofyear_dense_rank,
    s.product_id_avg_last_restock_weekofyear_p90,
    s.avg_product_id_avg_last_restock_weekofyear,
    s.row_count_rank,
    (select sum(i2.quantity) from inventory i2 where i2.store_id = s.store_id) as sum_inventory_quantity
from summary s;
