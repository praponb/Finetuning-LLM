with filtered_products as (
    select *, stock_qty - price as stock_qty_minus_price
    from products
    where price <= 750
),
agg as (
    select category, sum(case when brand = 'Initech' then 1 else 0 end) as initech_count
    from filtered_products
    group by category
)
select category, initech_count
from agg
