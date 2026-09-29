with valid as (
    -- Filter the raw rows and derive helper columns
    select *, cost + stock_qty cost_plus_stock_qty
    from products
    where launch_date >= date '2023-07-04'
      and supplier_id is not null
)
select supplier_id, brand
from valid
intersect all
select supplier_id, brand
from products
where cost between 530 and 660
