with enriched as (
    -- Attach products attributes
    select
        src.*,
        p.cost,
        p.stock_qty,
        p.launch_date
    from store_sales src
    left join products p
        on src.product_id = p.id
),
remaining_keys as (
    -- Set intersection on keys
    select product_id, store_id
    from enriched
    intersect all
    select product_id, store_id
    from store_sales
    where product_id in (2, 9)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join remaining_keys k on s.product_id = k.product_id and s.store_id = k.store_id
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        units,
        revenue,
        stock_qty,
        launch_date,
        dense_rank() over w_recent as stock_qty_dense_rank
    from kept_rows
    window w_recent as (partition by store_id, product_id order by stock_qty nulls last)
),
scored as (
    -- Ranking and gap features
    select
        id,
        launch_date,
        stock_qty_dense_rank,
        dense_rank() over (order by units desc nulls last) as units_dense_rank,
        percent_rank() over (order by stock_qty_dense_rank desc nulls last) as stock_qty_dense_rank_percent_rank,
        dense_rank() over (order by stock_qty_dense_rank desc) as stock_qty_dense_rank_dense_rank
    from metrics
),
enriched_store_sales as (
    -- Running and moving aggregates
    select
        stock_qty_dense_rank,
        stock_qty_dense_rank_percent_rank,
        stock_qty_dense_rank_dense_rank,
        rank() over (order by units_dense_rank nulls last) as units_dense_rank_rank,
        dense_rank() over (order by stock_qty_dense_rank_percent_rank desc nulls last) as stock_qty_dense_rank_percent_rank_dense_rank,
        cume_dist() over (order by stock_qty_dense_rank desc nulls last) as stock_qty_dense_rank_cume_dist
    from scored
    where stock_qty_dense_rank <= 5
)
select
    stock_qty_dense_rank,
    stock_qty_dense_rank_dense_rank,
    units_dense_rank_rank,
    stock_qty_dense_rank_percent_rank_dense_rank,
    stock_qty_dense_rank_cume_dist
from enriched_store_sales
order by stock_qty_dense_rank_dense_rank nulls first;
