WITH filtered AS (
    SELECT *, DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label
    FROM accounts
    WHERE opened_date > TO_DATE('2023-04-17')
),
enriched AS (
    SELECT
        src.id,
        src.customer_id,
        src.balance,
        src.opened_date,
        d.city,
        d.signup_date
    FROM filtered src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
enriched_accounts AS (
    SELECT
        id,
        signup_date,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY balance NULLS LAST) AS balance_percent_rank,
        NTILE(5) OVER (ORDER BY balance DESC, id) AS balance_bucket,
        LEAD(balance, 1, 0) OVER (ORDER BY balance, id) AS next_balance
    FROM enriched
),
with_latest AS (
    SELECT s.*, latest.transaction_txn_time
    FROM enriched_accounts s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    balance_bucket,
    next_balance,
    transaction_txn_time
FROM with_latest;
