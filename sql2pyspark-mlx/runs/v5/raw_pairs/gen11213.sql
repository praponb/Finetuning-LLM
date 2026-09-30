WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        status,
        INITCAP(TRIM(status)) status_title,
        COALESCE(status, 'N/A') status_filled
    FROM accounts
    WHERE opened_date >= TO_DATE('2023-03-21')
)
-- Anti-join via a correlated subquery
SELECT s.*
FROM valid s
WHERE NOT EXISTS (
        SELECT 1
        FROM transactions ch
        WHERE ch.account_id = s.id
          AND ch.txn_type = 'CREDIT'
    )
