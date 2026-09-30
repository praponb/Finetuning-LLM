WITH valid AS (
    SELECT
        *,
        CASE WHEN account_type = 'CHECKING' THEN 1 ELSE 0 END AS is_checking,
        SUBSTRING(account_type, 1, 4) AS account_type_prefix
    FROM accounts
    WHERE opened_date >= DATE '2023-04-20'
)
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    is_checking,
    account_type_prefix,
    LEAD(is_checking) OVER w_ordered AS next_is_checking,
    LEAD(balance) OVER w_ordered AS next_balance,
    LAG(balance) OVER w_ordered AS prev_balance,
    ROW_NUMBER() OVER w_ordered AS row_seq,
    SUM(balance) OVER (PARTITION BY status ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance
FROM valid
WINDOW w_ordered AS (PARTITION BY status, customer_id ORDER BY balance ASC, id ASC);
