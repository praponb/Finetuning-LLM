SELECT
    id,
    customer_id,
    status,
    CUME_DIST() OVER (PARTITION BY status, account_type ORDER BY balance DESC NULLS LAST) balance_cume_dist,
    RANK() OVER (ORDER BY balance DESC) balance_rank,
    NTILE(3) OVER (PARTITION BY account_type ORDER BY balance DESC, id) balance_bucket
FROM accounts
WHERE status NOT IN ('FROZEN', 'CLOSED');
