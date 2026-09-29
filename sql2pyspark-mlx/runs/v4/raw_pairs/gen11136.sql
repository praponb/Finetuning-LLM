WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY opened_date, id) AS event_seq,
        FIRST_VALUE(balance, TRUE) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance,
        SUM(balance) OVER (PARTITION BY account_type) AS account_type_sum_balance
    FROM accounts
),
with_windows_accounts AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        event_seq,
        first_known_balance,
        account_type_sum_balance,
        ROW_NUMBER() OVER (ORDER BY balance, id) AS row_seq,
        AVG(account_type_sum_balance) OVER (PARTITION BY status ORDER BY account_type_sum_balance, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS account_type_sum_balance_avg_6,
        LAG(account_type_sum_balance, 1, 0) OVER (PARTITION BY account_type ORDER BY opened_date, id) AS prev_account_type_sum_balance
    FROM with_windows
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.transaction_txn_time,
        x.transaction_amount
    FROM with_windows_accounts s
    JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.city,
        c.segment
    FROM latest_match a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN first_known_balance >= 64000 THEN 'HIGH' ELSE 'LOW' END AS first_known_balance_tier,
        SUM(CASE WHEN city = 'Bangkok' THEN account_type_sum_balance_avg_6 ELSE 0 END) AS bangkok_account_type_sum_balance_avg_6,
        SUM(event_seq) FILTER (WHERE account_type = 'CREDIT') AS credit_total_event_seq,
        COUNT(*) FILTER (WHERE account_type = 'CHECKING') AS checking_rows,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids,
        ROUND(SUM(first_known_balance) / NULLIF(COUNT(CASE WHEN status = 'OPEN' THEN 1 END), 0), 2) AS first_known_balance_per_open,
        (UNIX_TIMESTAMP(MAX(transaction_txn_time)) - UNIX_TIMESTAMP(MIN(transaction_txn_time))) / 60 AS span_minutes
    FROM with_dims
    GROUP BY CASE WHEN first_known_balance >= 64000 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    first_known_balance_tier,
    bangkok_account_type_sum_balance_avg_6,
    checking_rows
FROM totals
