with recent_inventory as (
    select *, floor(quantity) as quantity_whole
    from inventory
    where quantity between 79 and 81 and quantity <> 114
),
product_id_summary as (
    select product_id, round(stddev(quantity_whole), 2) as stddev_quantity_whole, max(quantity) as max_quantity
    from recent_inventory
    group by product_id
)
select *
from product_id_summary
order by max_quantity desc
