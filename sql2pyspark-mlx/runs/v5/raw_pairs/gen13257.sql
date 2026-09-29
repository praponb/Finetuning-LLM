with base as (
    -- Basic cleansing and defaults
    select *, ceil(quantity) as quantity_whole
    from inventory
    where product_id in (4, 5, 7, 9)
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by store_id order by quantity desc nulls last, id) as row_num
    from base
    qualify row_num = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        product_id,
        last_restock,
        avg(quantity) over (partition by product_id, store_id order by row_num, id rows between 6 preceding and current row) as quantity_avg_6,
        row_number() over w_running as row_seq,
        count(quantity) over (partition by store_id) as store_id_count_quantity,
        last_value(quantity_whole) ignore nulls over (order by quantity_whole, id rows between unbounded preceding and current row) as last_known_quantity_whole
    from deduped
    where quantity_whole is not null
    window w_running as (partition by product_id, store_id order by quantity, id)
),
enriched as (
    -- Attach products attributes
    select e.*, p.stock_qty
    from scored e
    left join products p
        on e.product_id = p.id
),
base_inventory as (
    -- Basic cleansing and defaults
    select *, round(quantity_avg_6 * 1.1, 2) as quantity_avg_6_adjusted
    from enriched
    where last_restock >= to_date('2023-02-17')
)
-- Multi-level aggregation
select
    date_trunc('month', last_restock) as activity_month,
    round(avg(quantity_avg_6), 2) as avg_quantity_avg_6,
    percentile_approx(row_seq, 0.95) as row_seq_p95,
    approx_percentile(store_id_count_quantity, array(0.25, 0.5, 0.75)) as store_id_count_quantity_quartiles
from base_inventory
group by 1
order by activity_month nulls last
