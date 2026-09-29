SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) AS rn FROM accounts) AS t WHERE rn <= 2;
