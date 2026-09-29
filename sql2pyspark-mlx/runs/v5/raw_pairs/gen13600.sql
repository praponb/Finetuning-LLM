with base as (
    -- Basic cleansing and defaults
    select *, round(stock_qty * 0.85, 2) as stock_qty_adjusted
    from products
    where launch_date > date '2023-04-22'
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, coalesce(stock_qty_adjusted, 0) as stock_qty_adjusted_filled
    from base
    where supplier_id is not null
),
enriched as (
    -- Attach suppliers attributes
    select
        e.*,
        p.country,
        p.rating,
        p.supplier_name
    from valid e
    left join suppliers p
        on e.supplier_id <=> p.id
),
valid_products as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        stock_qty_adjusted,
        stock_qty_adjusted_filled,
        country,
        supplier_name,
        datediff(current_date(), launch_date) as days_since_launch_date,
        case when cost >= 720 then 'High' when cost >= 300 then 'Medium' else 'Low' end as cost_tier,
        coalesce(brand, 'OTHER') as resolved_brand
    from enriched
    where brand not in ('Umbrella', 'Acme')
),
daily as (
    -- Aggregate with conditional metrics
    select
        category,
        round(sum(stock_qty_adjusted_filled) / nullif(count(case when brand = 'Acme' then 1 end), 0), 2) as stock_qty_adjusted_filled_per_acme,
        size(collect_set(category)) as num_categories,
        max(cost) filter (where category = 'Home') as home_max_cost,
        approx_percentile(days_since_launch_date, array(0.25, 0.5, 0.75)) as days_since_launch_date_quartiles,
        percentile_approx(cost, array(0.25, 0.5, 0.75)) as cost_quartiles,
        max(price) as max_price
    from valid_products
    group by 1
    having max(price) < 1000000
)
select
    category,
    stock_qty_adjusted_filled_per_acme,
    num_categories,
    home_max_cost,
    days_since_launch_date_quartiles,
    cost_quartiles,
    max_price
from daily
order by max_price;
