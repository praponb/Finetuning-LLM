SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS row_num FROM stores) AS t WHERE row_num <= 3 ORDER BY region;
