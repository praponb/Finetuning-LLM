WITH with_windows AS (
    SELECT
        *,
        AVG(balance) OVER w_part AS running_avg_balance,
        MIN(balance) OVER w_part AS running_min_balance,
        COUNT(balance) OVER w_part AS running_count_balance
    FROM accounts
    WHERE balance <= 20000
    WINDOW w_part AS (PARTITION BY account_type, customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    running_min_balance,
    CASE WHEN account_type RLIKE '^(SAVINGS|LOAN)$' THEN 'PRIORITY' WHEN account_type RLIKE '^(CHECKING|CREDIT)$' THEN 'CORE' ELSE 'EXTENDED' END AS account_type_zone
FROM with_windows
ORDER BY opened_date
