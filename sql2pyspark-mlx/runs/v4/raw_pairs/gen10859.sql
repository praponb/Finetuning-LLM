with src as (
    select *
    from inventory
    where store_id in (1, 3, 4, 6)
),
last_restock_quarter_summary as (
    select quarter(last_restock) as last_restock_quarter, round(avg(quantity), 2) as avg_quantity
    from src
    group by quarter(last_restock)
)
select *
from last_restock_quarter_summary
where avg_quantity > 71
order by last_restock_quarter desc, avg_quantity desc
