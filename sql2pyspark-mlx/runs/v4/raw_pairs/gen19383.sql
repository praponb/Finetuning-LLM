WITH cleaned AS (
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        LAST_DAY(opened_date) AS opened_date_month_end,
        COALESCE(status, 'DIRECT') AS status_clean
    FROM accounts
    WHERE status NOT IN ('OPEN', 'CLOSED')
)
SELECT s.*
FROM cleaned s
WHERE NOT EXISTS (
        SELECT 1
        FROM transactions x
        WHERE x.account_id = s.id
          AND x.txn_time < s.opened_date_month_end
    );
