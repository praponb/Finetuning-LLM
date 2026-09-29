with valid as (
    -- Keep recent, valid rows
    select
        *,
        round(units / revenue, 2) as revenue_per_unit,
        least(units, revenue) as least_units_revenue
    from store_sales
    where sale_date >= date '2023-05-26'
),
with_dims as (
    -- Attach products attributes
    select
        s.*,
        b.launch_date,
        b.cost,
        b.brand
    from valid s
    join products b
        on s.product_id = b.id
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from with_dims
    qualify rank() over (partition by store_id, product_id order by cost desc) = 1
),
metrics as (
    -- Ranking and gap features
    select
        id,
        product_id,
        units,
        revenue,
        revenue_per_unit,
        least_units_revenue,
        launch_date,
        cost,
        brand,
        sum(revenue) over w_running as last3_sum_revenue,
        count(revenue) over w_running as last3_count_revenue,
        min(revenue) over w_running as last3_min_revenue,
        sum(least_units_revenue) over (partition by brand) as brand_sum_least_units_revenue
    from deduped
    window w_running as (partition by store_id order by least_units_revenue, id rows between 2 preceding and current row)
),
enriched as (
    -- Attach products attributes
    select
        src.id,
        src.product_id,
        src.units,
        src.revenue,
        src.least_units_revenue,
        src.cost,
        src.brand,
        src.last3_count_revenue,
        src.brand_sum_least_units_revenue,
        p.brand as product_brand,
        p.stock_qty,
        p.cost as product_cost
    from metrics src
    join products p
        on src.product_id = p.id
)
-- Join a dimension and aggregate
select
    p.brand as product_brand,
    e.brand as store_sale_brand,
    count(*) as num_rows,
    max(e.product_cost) as max_product_cost,
    min(e.least_units_revenue) as min_least_units_revenue
from enriched e
join products p on e.product_id = p.id
group by p.brand, e.brand
order by product_brand
