select *, rank() over (partition by product_id order by last_restock desc nulls last) as dedup_rank from inventory qualify dedup_rank = 1;
