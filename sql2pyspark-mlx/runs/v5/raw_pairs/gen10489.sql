WITH valid AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        UPPER(account_type) AS account_type_upper,
        UPPER(status) AS status_upper
    FROM accounts
    WHERE status IS NOT NULL
      AND customer_id IN (20, 36)
)
SELECT *
FROM (
    SELECT
        status,
        account_type,
        balance
    FROM valid
) src
PIVOT (
    SUM(balance)
    FOR account_type IN ('CREDIT' AS credit, 'SAVINGS' AS savings, 'CHECKING' AS checking)
);
