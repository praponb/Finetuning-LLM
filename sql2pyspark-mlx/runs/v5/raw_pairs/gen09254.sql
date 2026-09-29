with ranked_inventory as (select *, row_number() over (partition by store_id order by last_restock, id) rn from inventory) select * from ranked_inventory where rn <= 3 order by last_restock;
