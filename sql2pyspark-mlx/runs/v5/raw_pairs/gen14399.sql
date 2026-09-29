SELECT id, account_type, balance FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS rn FROM accounts) AS t WHERE rn <= 2;
