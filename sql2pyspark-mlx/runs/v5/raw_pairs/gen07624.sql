with enriched as (
    -- Attach products attributes
    select
        src.*,
        d.category,
        d.stock_qty,
        d.product_name
    from inventory src
    left join products d
        on src.product_id = d.id
    where d.category in ('Electronics', 'Grocery')
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        length(product_name) as product_name_length,
        length(product_name) as product_name_length_2,
        coalesce(category, 'DIRECT') as resolved_category
    from enriched
    where last_restock >= date '2023-06-21'
),
scored as (
    -- Ranking and gap features
    select
        id,
        store_id,
        category,
        stock_qty,
        product_name_length,
        min(product_name_length) over (partition by category order by last_restock, id rows between 3 preceding and current row) as last4_min_product_name_length,
        sum(product_name_length) over (partition by category order by last_restock, id rows between 3 preceding and current row) as last4_sum_product_name_length,
        count(product_name_length) over (partition by category order by last_restock, id rows between 3 preceding and current row) as last4_count_product_name_length
    from base
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by store_id order by last4_min_product_name_length desc) as latest_rank
    from scored
    qualify latest_rank = 1
),
metrics as (
    -- Ranking and gap features
    select
        *,
        lag(stock_qty, 2, 0) over (partition by store_id order by last4_count_product_name_length, id) as prev_stock_qty,
        ntile(3) over (partition by category order by stock_qty desc, id) as stock_qty_bucket
    from latest
),
joined as (
    -- Attach stores attributes
    select
        s.id,
        s.store_id,
        s.product_name_length,
        s.last4_sum_product_name_length,
        s.last4_count_product_name_length,
        s.latest_rank,
        s.prev_stock_qty,
        b.store_name,
        b.opened_date
    from metrics s
    inner join stores b
        on s.store_id = b.id
),
scored_inventory as (
    -- Running and moving aggregates
    select
        *,
        sum(last4_count_product_name_length) over (partition by store_id order by latest_rank, id rows between unbounded preceding and current row) as cumulative_last4_count_product_name_length,
        sum(last4_count_product_name_length) over (partition by store_id order by last4_count_product_name_length, id rows between unbounded preceding and current row) as running_last4_count_product_name_length,
        percent_rank() over (partition by store_id order by last4_sum_product_name_length nulls last) as last4_sum_product_name_length_percent_rank,
        ntile(5) over (order by prev_stock_qty, id) as prev_stock_qty_bucket,
        first_value(prev_stock_qty, true) over (partition by store_id order by last4_count_product_name_length, id rows between unbounded preceding and current row) as first_known_prev_stock_qty
    from joined
)
-- Ranking and gap features
select
    *,
    max(prev_stock_qty) over w_part as last4_max_prev_stock_qty,
    avg(prev_stock_qty) over w_part as last4_avg_prev_stock_qty,
    sum(prev_stock_qty) over w_part as last4_sum_prev_stock_qty,
    sum(first_known_prev_stock_qty) over (partition by store_id order by last4_sum_product_name_length_percent_rank, id rows between unbounded preceding and current row) as running_first_known_prev_stock_qty
from scored_inventory
window w_part as (partition by store_id order by first_known_prev_stock_qty, id rows between 3 preceding and current row);
