WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(status) AS status_upper,
        COALESCE(status, 'N/A') AS status_filled
    FROM accounts
    WHERE status IS NOT NULL
)
-- Anti-join via a correlated subquery
SELECT s.*
FROM filtered s
WHERE NOT EXISTS (
        SELECT 1
        FROM transactions ch
        WHERE ch.account_id = s.id
          AND HOUR(ch.txn_time) BETWEEN 6 AND 11
    );
