SELECT * FROM (SELECT id, region, opened_date, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS pos FROM stores) AS ranked WHERE pos <= 5
