SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) row_num FROM stores) t WHERE row_num <= 3 ORDER BY id, region
