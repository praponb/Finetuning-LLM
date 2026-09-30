with base as (
    -- Basic cleansing and defaults
    select *, last_day(last_restock) as last_restock_month_end
    from inventory
    where last_restock > date '2023-04-30'
      and product_id in (4, 9, 10)
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        avg(quantity) over w_recent as quantity_avg_5_prior,
        ntile(5) over (order by quantity desc, id) as quantity_bucket
    from base
    window w_recent as (order by quantity, id rows between 5 preceding and 1 preceding)
),
with_dims as (
    -- Attach products attributes
    select
        a.id,
        a.product_id,
        a.quantity,
        a.quantity_avg_5_prior,
        b.stock_qty,
        b.category
    from scored a
    inner join products b
        on a.product_id = b.id
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        quantity,
        quantity_avg_5_prior,
        category,
        first_value(quantity_avg_5_prior) ignore nulls over (partition by category order by quantity, id rows between unbounded preceding and current row) as first_known_quantity_avg_5_prior,
        avg(stock_qty) over (partition by category order by quantity, id rows between 3 preceding and current row) as stock_qty_avg_3
    from with_dims
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        quantity,
        quantity_avg_5_prior,
        category,
        stock_qty_avg_3,
        round(quantity, 2) as quantity_rounded
    from with_windows
    where category is not null
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by category order by stock_qty_avg_3 nulls last, id) as rn
        from filtered
    ) t
    where rn = 1
),
scored_inventory as (
    -- Running and moving aggregates
    select
        id,
        quantity_avg_5_prior,
        category,
        stock_qty_avg_3,
        rn,
        dense_rank() over (partition by category order by quantity) as quantity_dense_rank,
        lead(quantity_rounded, 1) over (order by quantity, id) as next_quantity_rounded
    from deduped
    where category not in ('Grocery', 'Electronics')
)
select
    id,
    category,
    stock_qty_avg_3,
    quantity_dense_rank,
    next_quantity_rounded
from scored_inventory
