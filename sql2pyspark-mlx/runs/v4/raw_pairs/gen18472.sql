with Ranked as (select id, store_id, last_restock, rank() over (partition by store_id order by last_restock desc) as row_num from inventory) select * from Ranked where row_num <= 2;
