WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < s.opened_date
        )
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.balance,
    s.status,
    (SELECT MAX(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM not_flagged s
ORDER BY account_type NULLS LAST
