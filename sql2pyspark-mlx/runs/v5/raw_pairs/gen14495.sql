SELECT id, store_id, units FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC NULLS FIRST, id) AS dedup_rank FROM store_sales) x WHERE dedup_rank = 1
