select store_id, count(*) as cnt, min(last_restock) as min_last_restock from inventory where last_restock between '2023-11-01' and '2024-09-01' group by store_id order by store_id desc
