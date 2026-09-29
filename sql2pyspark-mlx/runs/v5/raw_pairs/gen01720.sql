SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC NULLS LAST, id) AS dedup_rank FROM inventory QUALIFY dedup_rank = 1;
