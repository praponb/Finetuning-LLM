with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        substring(category, 1, 2) as category_prefix,
        trim(category) as clean_category
    from products
    where launch_date >= date '2023-10-16'
)
-- Ranking and gap features
select
    *,
    avg(cost) over (partition by category order by launch_date, id rows between 6 preceding and 1 preceding) as cost_avg_6_prior,
    percent_rank() over (order by stock_qty nulls last) as stock_qty_percent_rank,
    corr(cost, stock_qty) over (partition by category, brand) as corr_cost_stock_qty
from base;
