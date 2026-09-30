with base as (
    select
        id,
        product_name,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        stock_qty - price stock_qty_minus_price,
        regexp_replace(product_name, ' ', '-') product_name_slug
    from products
    where launch_date >= date '2023-03-02'
      and supplier_id is not null
),
with_windows as (
    select
        *,
        sum(stock_qty) over (partition by supplier_id order by stock_qty_minus_price, id rows between unbounded preceding and current row) cumulative_stock_qty,
        lead(stock_qty, 1) over (partition by supplier_id order by cost, id) next_stock_qty,
        row_number() over (partition by category order by stock_qty desc, id) row_seq
    from base
    where cost > 510
),
kept_keys as (
    select id, category
    from with_windows
    except distinct
    select id, category
    from products
    where product_name like 'Noi%'
),
reconciled as (
    select s.*
    from with_windows s
    join kept_keys k on s.id = k.id and s.category = k.category
),
with_windows_products as (
    select
        id,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        cumulative_stock_qty,
        row_seq,
        stddev_samp(stock_qty) over (partition by category, supplier_id) category_stddev_samp_stock_qty,
        count(next_stock_qty) over w_hist category_count_next_stock_qty,
        rank() over (partition by category order by cumulative_stock_qty desc nulls last) cumulative_stock_qty_rank,
        percent_rank() over (order by stock_qty desc) stock_qty_percent_rank,
        max(row_seq) over w_hist category_max_row_seq
    from reconciled
    window w_hist as (partition by category)
),
kept_keys_2 as (
    select id, supplier_id
    from with_windows_products
    intersect all
    select id, supplier_id
    from products
    where brand <> 'Stark'
),
verified as (
    select s.*
    from with_windows_products s
    join kept_keys_2 k on s.id = k.id and s.supplier_id = k.supplier_id
)
select
    id,
    price,
    cost,
    stock_qty,
    tags,
    cumulative_stock_qty,
    row_seq,
    category_stddev_samp_stock_qty,
    category_count_next_stock_qty
from verified
order by category_stddev_samp_stock_qty nulls last
