WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END AS is_frozen
    FROM accounts
    WHERE opened_date >= DATE '2023-03-22'
)
-- Anti-join via a correlated subquery
SELECT s.*
FROM valid s
WHERE NOT EXISTS (
        SELECT 1
        FROM transactions c
        WHERE c.account_id = s.id
          AND c.txn_time >= s.opened_date
    );
