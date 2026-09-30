with base as (
    -- Basic cleansing and defaults
    select
        *,
        cast(units as double) as units_num,
        ceil(revenue) as revenue_whole
    from store_sales
    where sale_date >= date '2023-07-17'
      and sale_date > '2024-09-13'
),
joined as (
    -- Attach products attributes
    select
        a.id,
        a.store_id,
        a.product_id,
        a.units_num,
        a.revenue_whole,
        p.product_name,
        p.category
    from base a
    left join products p
        on a.product_id = p.id
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by category order by units_num desc, id) as rn
    from joined
    qualify rn = 1
),
base_store_sales as (
    -- Keep recent, valid rows
    select
        id,
        product_id,
        revenue_whole,
        product_name,
        category,
        greatest(revenue_whole, rn) as greatest_revenue_whole_rn,
        substring(product_name, 1, 4) as product_name_prefix
    from ranked
    where rn != 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        avg(greatest_revenue_whole_rn) over (order by greatest_revenue_whole_rn, id rows between 2 preceding and 1 preceding) as greatest_revenue_whole_rn_avg_2_prior,
        percent_rank() over (order by greatest_revenue_whole_rn desc nulls last) as greatest_revenue_whole_rn_percent_rank
    from base_store_sales
)
select
    s.id,
    s.product_id,
    s.product_name_prefix,
    s.greatest_revenue_whole_rn_avg_2_prior,
    (select avg(t2.quantity) from inventory t2 where t2.product_id = s.product_id) as avg_inventory_quantity
from metrics s
order by greatest_revenue_whole_rn_avg_2_prior desc nulls last, avg_inventory_quantity;
