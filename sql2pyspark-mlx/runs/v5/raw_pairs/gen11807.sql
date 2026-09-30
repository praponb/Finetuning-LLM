SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount, id) AS dedup_rank FROM payments QUALIFY dedup_rank = 1;
