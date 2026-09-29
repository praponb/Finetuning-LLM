SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC, id) AS dedup_rank
FROM products
QUALIFY dedup_rank = 1
