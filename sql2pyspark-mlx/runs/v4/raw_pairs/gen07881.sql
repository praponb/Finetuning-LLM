with remaining_keys as (
    -- Set difference on keys
    select id
    from inventory
    except distinct
    select id
    from inventory
    where month(last_restock) in (1, 4, 9)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from inventory s
    inner join remaining_keys k on s.id = k.id
)
select
    s.id,
    s.store_id,
    s.product_id,
    s.last_restock,
    (select sum(i2.quantity) from inventory i2 where i2.product_id = s.product_id) as sum_inventory_quantity
from verified s
order by sum_inventory_quantity desc;
