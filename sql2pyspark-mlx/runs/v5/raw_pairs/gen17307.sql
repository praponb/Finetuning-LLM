WITH latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.transaction_merchant
    FROM accounts s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Grouped metrics
SELECT
    customer_id,
    COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS open_count,
    MAX(balance) AS max_balance,
    SIZE(COLLECT_SET(status)) AS num_status,
    SORT_ARRAY(COLLECT_SET(transaction_merchant)) AS transaction_merchant_list,
    SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
    SUM(CASE WHEN account_type = 'CHECKING' THEN balance ELSE 0 END) AS checking_balance
FROM latest_match
GROUP BY customer_id;
