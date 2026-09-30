SELECT id, region, opened_date, sqft FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) rn FROM stores) t WHERE rn <= 2;
