with with_last as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.inventory_last_restock,
        x.quantity
    from products s
    join lateral (
            select c.last_restock as inventory_last_restock, c.quantity
            from inventory c
            where c.product_id = s.id
            order by c.last_restock desc, c.id desc
            limit 1
        ) x on true
),
latest_match as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.category,
        s.price,
        s.cost,
        s.stock_qty,
        s.launch_date,
        s.inventory_last_restock,
        x.order_item_quantity
    from with_last s
    left join lateral (
            select c.quantity as order_item_quantity
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        category,
        cost,
        stock_qty,
        launch_date,
        avg(stock_qty) over (order by cost, id rows between 6 preceding and 1 preceding) as stock_qty_avg_6_prior,
        min(cost) over (partition by category) as category_min_cost,
        round(order_item_quantity / sum(order_item_quantity) over (partition by category), 4) as order_item_quantity_share,
        lead(stock_qty, 1) over (partition by category order by order_item_quantity, id) as next_stock_qty
    from latest_match
    where launch_date < '2023-05-01'
),
with_windows_products as (
    -- Ranking and gap features
    select
        id,
        stock_qty,
        launch_date,
        stock_qty_avg_6_prior,
        avg(order_item_quantity_share) over (partition by category order by stock_qty_avg_6_prior, id rows between 6 preceding and current row) as order_item_quantity_share_avg_6,
        avg(cost) over (order by cost, id rows between 5 preceding and 1 preceding) as cost_avg_5_prior,
        row_number() over (order by cost desc, id) as row_seq,
        lag(next_stock_qty, 1) over (partition by category order by stock_qty_avg_6_prior, id) as prev_next_stock_qty,
        row_number() over (partition by category order by stock_qty_avg_6_prior desc, id) as seq_no
    from with_windows
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(prev_next_stock_qty * 1.15, 2) as prev_next_stock_qty_adjusted,
        add_months(launch_date, 3) as launch_date_plus_months
    from with_windows_products
    where launch_date > date '2023-05-21'
      and launch_date > '2024-07-18'
),
with_latest as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select s.*, latest.quantity
    from base s
    left join lateral (
            select c.quantity
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) latest on true
),
base_products as (
    -- Keep recent, valid rows
    select
        id,
        launch_date,
        stock_qty_avg_6_prior,
        order_item_quantity_share_avg_6,
        cost_avg_5_prior,
        row_seq,
        seq_no,
        prev_next_stock_qty_adjusted,
        quantity,
        stock_qty * order_item_quantity_share_avg_6 as stock_qty_times_order_item_quantity_share_avg_6,
        coalesce(stock_qty, 0) as stock_qty_filled
    from with_latest
    where launch_date between '2023-09-01' and '2024-11-01'
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        stock_qty_times_order_item_quantity_share_avg_6,
        stock_qty_filled,
        prev_next_stock_qty_adjusted
    from base_products
) src
unpivot (
    reading for metric in (stock_qty_times_order_item_quantity_share_avg_6, stock_qty_filled, prev_next_stock_qty_adjusted)
) unp;
