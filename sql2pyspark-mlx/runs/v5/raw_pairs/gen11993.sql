SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY opened_date DESC NULLS LAST, id) AS rn
FROM stores
QUALIFY rn = 1;
