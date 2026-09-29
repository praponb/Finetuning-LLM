WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_time_label
    FROM transactions
    WHERE merchant IS NOT NULL
      AND merchant = 'Shell'
),
with_dims AS (
    -- Attach accounts attributes
    SELECT e.*, d.account_type
    FROM cleaned e
    INNER JOIN accounts d
        ON e.account_id = d.id
    WHERE d.status <> 'FROZEN'
),
joined AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.txn_type,
        a.amount,
        a.txn_time,
        a.txn_time_label,
        a.account_type,
        d.status,
        d.balance
    FROM with_dims a
    LEFT JOIN accounts d
        ON a.account_id = d.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(balance, 1) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS next_balance,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        AVG(balance) OVER (PARTITION BY account_id, status ORDER BY txn_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS balance_avg_6
    FROM joined
),
joined_transactions AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.txn_time,
        s.next_balance,
        s.balance_avg_6,
        d.balance AS account_balance,
        d.status AS account_status
    FROM scored s
    LEFT JOIN accounts d
        ON s.account_id = d.id
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            account_id,
            account_status,
            account_balance
        FROM joined_transactions
    ) src
    PIVOT (
        SUM(account_balance) AS value_sum
        FOR account_status IN ('CLOSED' AS closed, 'FROZEN' AS frozen, 'OPEN' AS open)
    )
)
SELECT
    s.account_id,
    s.frozen,
    s.open,
    (SELECT AVG(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS avg_transaction_amount
FROM pivoted s
ORDER BY account_id, frozen
