select id, store_id, last_restock
from (
    select
        id,
        store_id,
        last_restock,
        rank() over (partition by store_id order by last_restock desc) as rnk
    from inventory
) as ranked
where rnk = 1;
