WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        CONCAT_WS('/', account_type, status) AS account_type_status_key
    FROM accounts
    WHERE opened_date > DATE '2023-03-06'
      AND status IS NOT NULL
)
SELECT id, status
FROM cleaned
EXCEPT DISTINCT
SELECT id, status
FROM accounts
WHERE account_type IN ('SAVINGS', 'LOAN', 'CHECKING')
