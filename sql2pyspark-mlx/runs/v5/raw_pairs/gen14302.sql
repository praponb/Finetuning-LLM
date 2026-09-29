SELECT id, store_name FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY opened_date NULLS FIRST, id) AS latest_rank FROM stores) t WHERE latest_rank = 1
