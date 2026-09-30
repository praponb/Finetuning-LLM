with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        revenue * units as revenue_per_unit,
        units + revenue as revenue_per_unit_2
    from store_sales
    where sale_date >= date '2023-05-08'
),
enriched as (
    -- Attach products attributes
    select
        src.*,
        b.brand,
        b.category,
        b.price
    from cleaned src
    left join products b
        on src.product_id = b.id
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(brand, 'Unknown') as brand_filled,
        round(revenue_per_unit, 2) as revenue_per_unit_rounded
    from enriched
    where sale_date >= to_date('2023-09-05')
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        product_id,
        sale_date,
        revenue_per_unit_2,
        brand,
        category,
        brand_filled,
        revenue_per_unit_rounded,
        floor(revenue_per_unit_rounded) as revenue_per_unit_rounded_whole,
        round(price * 1.07, 2) as price_scaled,
        coalesce(category, 'OTHER') as category_filled
    from base
    where sale_date >= date '2023-02-18'
      and category is not null
),
enriched_store_sales as (
    -- Attach products attributes
    select
        src.id,
        src.product_id,
        src.revenue_per_unit_2,
        src.category,
        src.brand_filled,
        src.price_scaled,
        src.category_filled,
        p.cost
    from valid src
    left join products p
        on src.product_id = p.id
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by category order by price_scaled, id) as row_num
    from enriched_store_sales
    qualify row_num = 1
),
metrics as (
    -- Ranking and gap features
    select
        id,
        product_id,
        revenue_per_unit_2,
        category,
        brand_filled,
        category_filled,
        count(revenue_per_unit_2) over (partition by category order by revenue_per_unit_2, id rows between 2 preceding and current row) as last3_count_revenue_per_unit_2,
        sum(revenue_per_unit_2) over (partition by category order by revenue_per_unit_2, id rows between 2 preceding and current row) as last3_sum_revenue_per_unit_2,
        max(revenue_per_unit_2) over (partition by category order by revenue_per_unit_2, id rows between 2 preceding and current row) as last3_max_revenue_per_unit_2,
        cume_dist() over (partition by category, product_id order by price_scaled desc) as price_scaled_cume_dist
    from ranked
    where revenue_per_unit_2 >= 42
)
select
    id,
    revenue_per_unit_2,
    category,
    category_filled,
    last3_count_revenue_per_unit_2,
    last3_sum_revenue_per_unit_2,
    case when category rlike '^(Grocery|Electronics)$' then 'GROUP_B' when category rlike '^(Home|Sports)$' then 'OTHER_ZONE' else 'CORE' end as category_zone
from metrics
order by last3_sum_revenue_per_unit_2 desc;
