with top_inventory as (
    select *, rank() over (partition by store_id order by last_restock desc) as rnk
    from inventory
)
select *
from top_inventory
where rnk <= 3;
