with valid as (
    select
        *,
        coalesce(stock_qty, 0) as stock_qty_filled,
        coalesce(brand, 'NONE') as brand_clean
    from products
    where supplier_id is not null
      and cost < 260
)
select
    *,
    lag(price, 1) over (partition by supplier_id order by launch_date asc, id asc) as prev_price,
    sum(cost) over (partition by brand order by stock_qty asc, id asc rows between unbounded preceding and current row) as running_cost,
    lag(cost, 1) over (partition by supplier_id order by launch_date asc, id asc) as prev_cost
from valid
