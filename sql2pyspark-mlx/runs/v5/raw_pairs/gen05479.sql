WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        opened_date,
        LEAD(balance) OVER (PARTITION BY customer_id, account_type ORDER BY balance, id) AS next_balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY balance, id) AS seq_no,
        RANK() OVER (ORDER BY balance DESC NULLS FIRST) AS balance_rank
    FROM accounts
    WHERE account_type != 'LOAN'
)
SELECT
    s.id,
    s.customer_id,
    s.opened_date,
    s.next_balance,
    s.balance_rank,
    (SELECT MIN(t2.shipping_fee) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS min_order_shipping_fee
FROM with_windows s
ORDER BY next_balance DESC, balance_rank
