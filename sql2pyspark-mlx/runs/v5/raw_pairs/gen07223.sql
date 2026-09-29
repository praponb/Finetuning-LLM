with cleaned as (
    select
        id,
        brand,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        add_months(launch_date, 6) as launch_date_plus_months
    from products
    where supplier_id is not null
),
with_windows as (
    select
        *,
        avg(stock_qty) over (partition by brand order by launch_date_plus_months, id rows between 5 preceding and current row) as stock_qty_avg_5,
        sum(cost) over (partition by brand order by launch_date_plus_months, id rows between unbounded preceding and current row) as cumulative_cost,
        corr(cost, stock_qty) over (partition by supplier_id) as corr_cost_stock_qty,
        avg(cost) over (partition by brand order by launch_date, id rows between 3 preceding and current row) as cost_avg_3,
        min(stock_qty) over (partition by brand) as brand_min_stock_qty
    from cleaned
    where launch_date < '2025-07-10'
),
cleaned_products as (
    select
        id,
        brand,
        cost,
        stock_qty,
        tags,
        supplier_id,
        cumulative_cost,
        corr_cost_stock_qty,
        brand_min_stock_qty,
        coalesce(stock_qty_avg_5, 0) as stock_qty_avg_5_filled,
        coalesce(brand, 'N/A') as resolved_brand
    from with_windows
    where launch_date >= date '2023-05-04'
      and supplier_id is not null
),
enriched as (
    select
        e.id,
        e.brand,
        e.cost,
        e.stock_qty,
        e.tags,
        e.supplier_id,
        e.cumulative_cost,
        e.brand_min_stock_qty,
        e.stock_qty_avg_5_filled,
        p.supplier_name,
        p.rating,
        p.country
    from cleaned_products e
    left join suppliers p
        on e.supplier_id <=> p.id
),
filtered as (
    select
        id,
        brand,
        cost,
        stock_qty,
        tags,
        supplier_id,
        brand_min_stock_qty,
        supplier_name,
        rating,
        country,
        greatest(cumulative_cost, brand_min_stock_qty) as greatest_cumulative_cost_brand_min_stock_qty,
        round(stock_qty_avg_5_filled / rating, 2) as stock_qty_avg_5_filled_per_rating,
        coalesce(brand, 'OTHER') as brand_filled
    from enriched
    where cumulative_cost is not null
)
select
    id,
    brand,
    cost,
    tags,
    brand_min_stock_qty,
    greatest_cumulative_cost_brand_min_stock_qty,
    stock_qty_avg_5_filled_per_rating,
    brand_filled,
    case when brand rlike '^(Initech|Umbrella)$' then 'CORE' else 'GROUP_A' end as brand_zone
from filtered
order by brand_zone;
