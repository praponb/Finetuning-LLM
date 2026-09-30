with base as (
    -- Keep recent, valid rows
    select
        *,
        greatest(cost, stock_qty) as greatest_cost_stock_qty,
        coalesce(brand, 'NONE') as resolved_brand
    from products
    where launch_date > to_date('2023-04-09')
),
kept_keys as (
    -- Set difference on keys
    select id, supplier_id
    from base
    except distinct
    select id, supplier_id
    from products
    where stock_qty >= 219
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join kept_keys k on s.id = k.id and s.supplier_id = k.supplier_id
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        min(stock_qty) over (partition by supplier_id) as supplier_id_min_stock_qty,
        corr(cost, stock_qty) over (partition by category, supplier_id) as corr_cost_stock_qty,
        rank() over w_running as stock_qty_rank,
        percent_rank() over (partition by brand order by greatest_cost_stock_qty desc nulls first) as greatest_cost_stock_qty_percent_rank,
        rank() over w_recent as cost_rank
    from verified
    window w_running as (partition by category, supplier_id order by stock_qty nulls last), w_recent as (order by cost nulls first)
),
metrics as (
    -- Ranking and gap features
    select
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        supplier_id,
        resolved_brand,
        supplier_id_min_stock_qty,
        corr_cost_stock_qty,
        cost_rank,
        round(cost_rank / sum(cost_rank) over (partition by category), 4) as cost_rank_share
    from enriched
    where array_contains(tags, 'sale')
),
enriched_products as (
    -- Running and moving aggregates
    select
        product_name,
        category,
        stock_qty,
        launch_date,
        supplier_id,
        resolved_brand,
        cost_rank_share,
        cume_dist() over (partition by supplier_id order by corr_cost_stock_qty desc nulls first) as corr_cost_stock_qty_cume_dist,
        avg(cost_rank) over (partition by brand) as brand_avg_cost_rank,
        dense_rank() over (partition by supplier_id order by cost_rank_share desc nulls first) as cost_rank_share_dense_rank,
        percent_rank() over (order by cost_rank_share nulls first) as cost_rank_share_percent_rank,
        dense_rank() over (order by corr_cost_stock_qty desc nulls last) as corr_cost_stock_qty_dense_rank
    from metrics
),
base_products as (
    -- Keep recent, valid rows
    select
        *,
        corr_cost_stock_qty_cume_dist - cost_rank_share_percent_rank as corr_cost_stock_qty_cume_dist_minus_cost_rank_share_percent_rank,
        concat(product_name, ' ', category) as product_name_category
    from enriched_products
    where launch_date >= date '2023-08-22'
      and supplier_id is not null
),
with_dims as (
    -- Attach suppliers attributes
    select
        a.product_name,
        a.category,
        a.stock_qty,
        a.supplier_id,
        a.resolved_brand,
        a.brand_avg_cost_rank,
        a.cost_rank_share_dense_rank,
        p.country
    from base_products a
    left join suppliers p
        on a.supplier_id = p.id
)
-- Native SQL PIVOT
select *
from (
    select
        category,
        country,
        cost_rank_share_dense_rank
    from with_dims
) src
pivot (
    max(cost_rank_share_dense_rank) as amount,
    count(1) as cnt
    for country in ('DE', 'US')
);
