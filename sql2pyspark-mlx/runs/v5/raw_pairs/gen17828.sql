select *
from (
    select
        quarter(last_restock) as last_restock_quarter,
        store_id,
        count(quantity) as quantity_count,
        max(quantity) as max_quantity
    from inventory
    where quantity between 72 and 210
    group by quarter(last_restock), store_id
) as x
where max_quantity >= 160;
