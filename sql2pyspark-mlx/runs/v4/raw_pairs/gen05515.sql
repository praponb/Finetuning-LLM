with ranked_inventory as (
    select *, rank() over (partition by product_id order by last_restock desc) as rnk
    from inventory
)
select *
from ranked_inventory
where rnk <= 3
order by product_id desc, last_restock desc;
