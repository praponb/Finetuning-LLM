WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS dedup_rank FROM inventory QUALIFY dedup_rank = 1) SELECT id, product_id, dedup_rank FROM ranked
