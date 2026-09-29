WITH base AS (
    SELECT
        *,
        LOWER(account_type) account_type_lower,
        SUBSTRING(account_type, 1, 4) account_type_prefix
    FROM accounts
    WHERE opened_date >= DATE '2023-04-08'
      AND customer_id IS NOT NULL
)
SELECT s.*
FROM base s
WHERE NOT EXISTS (
        SELECT 1
        FROM transactions x
        WHERE x.account_id = s.id
          AND x.txn_time >= s.opened_date
    );
