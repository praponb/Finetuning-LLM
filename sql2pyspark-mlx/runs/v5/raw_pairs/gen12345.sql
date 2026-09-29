with ValidInventory as (
    select *
    from inventory
    where last_restock < '2024-07-13' and quantity < 232
),
inventory_stats as (
    select store_id, max(quantity) max_quantity
    from ValidInventory
    group by store_id
)
select *
from inventory_stats
where max_quantity >= 80
order by store_id
limit 5;
