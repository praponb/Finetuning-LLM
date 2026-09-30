SELECT *, RANK() OVER (PARTITION BY account_type, status ORDER BY balance DESC) AS latest_rank FROM accounts QUALIFY latest_rank = 1;
