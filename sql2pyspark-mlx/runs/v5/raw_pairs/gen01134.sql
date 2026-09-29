WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END AS is_frozen,
        LOWER(status) AS status_lower
    FROM accounts
    WHERE status IS NOT DISTINCT FROM 'FROZEN'
      AND opened_date >= DATE '2023-05-26'
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.transaction_txn_time
    FROM filtered s
    JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        balance,
        opened_date,
        status_lower,
        AVG(is_frozen) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS is_frozen_avg_6_prior,
        COUNT(is_frozen) OVER w_recent AS is_frozen_count_90d,
        AVG(balance) OVER w_recent_2 AS customer_id_avg_balance
    FROM with_last
    WINDOW w_recent AS (ORDER BY CAST(transaction_txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW), w_recent_2 AS (PARTITION BY customer_id)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        opened_date,
        status_lower,
        is_frozen_avg_6_prior,
        is_frozen_count_90d,
        customer_id_avg_balance,
        COALESCE(customer_id_avg_balance, 0) AS customer_id_avg_balance_filled
    FROM scored
    WHERE opened_date > DATE '2023-08-10'
)
SELECT
    id,
    opened_date,
    status_lower,
    is_frozen_avg_6_prior,
    is_frozen_count_90d,
    customer_id_avg_balance,
    customer_id_avg_balance_filled
FROM cleaned
ORDER BY customer_id_avg_balance_filled DESC NULLS FIRST;
