SELECT *, RANK() OVER (PARTITION BY merchant, account_id ORDER BY amount DESC NULLS LAST) AS dedup_rank
FROM transactions
QUALIFY dedup_rank = 1;
