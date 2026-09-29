with enriched as (
    -- Attach products attributes
    select
        s.*,
        b.stock_qty,
        b.launch_date
    from order_items s
    left join products b
        on s.product_id = b.id
    where b.brand in ('Globex', 'Initech', 'Stark')
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        order_id,
        product_id,
        stock_qty,
        launch_date,
        sum(stock_qty) over (partition by order_id order by unit_price, id rows between unbounded preceding and current row) as running_sum_stock_qty,
        avg(stock_qty) over (partition by order_id order by unit_price, id rows between unbounded preceding and current row) as running_avg_stock_qty,
        min(stock_qty) over (partition by order_id order by unit_price, id rows between unbounded preceding and current row) as running_min_stock_qty,
        cume_dist() over (order by unit_price desc nulls last) as unit_price_cume_dist
    from enriched
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from with_windows
    qualify row_number() over (partition by product_id order by running_sum_stock_qty desc nulls last, id) = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        running_sum_stock_qty,
        running_avg_stock_qty,
        running_min_stock_qty,
        min(running_avg_stock_qty) over (partition by order_id, product_id order by running_avg_stock_qty, id rows between 3 preceding and current row) as last4_min_running_avg_stock_qty,
        sum(running_avg_stock_qty) over (partition by order_id, product_id order by running_avg_stock_qty, id rows between 3 preceding and current row) as last4_sum_running_avg_stock_qty,
        count(running_avg_stock_qty) over (partition by order_id, product_id order by running_avg_stock_qty, id rows between 3 preceding and current row) as last4_count_running_avg_stock_qty,
        dense_rank() over (partition by product_id order by stock_qty nulls last) as stock_qty_dense_rank
    from deduped
)
-- Ranking and gap features
select
    id,
    running_sum_stock_qty,
    running_avg_stock_qty,
    running_min_stock_qty,
    last4_sum_running_avg_stock_qty,
    last4_count_running_avg_stock_qty,
    row_number() over (order by last4_count_running_avg_stock_qty desc, id) as row_seq,
    cume_dist() over (order by stock_qty_dense_rank desc nulls last) as stock_qty_dense_rank_cume_dist,
    ntile(3) over (order by running_avg_stock_qty, id) as running_avg_stock_qty_bucket,
    sum(running_min_stock_qty) over (order by running_sum_stock_qty, id rows between unbounded preceding and current row) as cumulative_running_min_stock_qty
from metrics;
