with valid_inventory as (
    select *, last_day(last_restock) as last_restock_month_end
    from inventory
    where store_id in (4, 5, 6)
),
summary as (
    select year(last_restock_month_end) as last_restock_month_end_year, round(avg(quantity), 2) as avg_quantity
    from valid_inventory
    group by year(last_restock_month_end)
)
select last_restock_month_end_year, avg_quantity
from summary
