with ranked_inventory as (
    select
        id,
        store_id,
        quantity,
        last_restock,
        row_number() over (partition by store_id order by quantity desc, id) as rnk
    from inventory
)
select *
from ranked_inventory
where rnk <= 2
order by store_id asc
