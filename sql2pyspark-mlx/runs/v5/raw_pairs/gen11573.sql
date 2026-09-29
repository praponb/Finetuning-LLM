WITH eligible AS (
    SELECT s.*
    FROM accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
)
SELECT
    id,
    account_type,
    balance,
    opened_date
FROM eligible;
