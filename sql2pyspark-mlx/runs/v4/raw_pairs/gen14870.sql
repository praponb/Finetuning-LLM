SELECT * FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY balance DESC) row_num FROM accounts) t WHERE row_num <= 1
