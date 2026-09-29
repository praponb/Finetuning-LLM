WITH valid AS (
    SELECT
        *,
        LOWER(status) AS status_lower,
        ROUND(balance, 1) AS balance_rounded,
        COALESCE(status, 'N/A') AS resolved_status
    FROM accounts
    WHERE status IS NOT NULL
)
SELECT
    *,
    RANK() OVER (PARTITION BY status, account_type ORDER BY balance DESC) AS balance_rank,
    AVG(balance_rounded) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS balance_rounded_avg_3,
    MAX(balance_rounded) OVER (PARTITION BY customer_id, status) AS customer_id_max_balance_rounded
FROM valid
