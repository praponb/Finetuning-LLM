WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        LAST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY status, customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance,
        LEAD(balance, 1) OVER w_running AS next_balance
    FROM accounts
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY balance, id)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date, id) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
enriched_accounts AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY status, customer_id ORDER BY balance DESC NULLS LAST) AS balance_dense_rank,
        AVG(balance) OVER (PARTITION BY customer_id) AS customer_id_avg_balance,
        RANK() OVER (PARTITION BY status, customer_id ORDER BY balance DESC) AS balance_rank
    FROM latest
)
-- Window metrics over several different partitions
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    last_known_balance,
    next_balance,
    row_num,
    balance_dense_rank,
    customer_id_avg_balance,
    balance_rank,
    SUM(balance_dense_rank) OVER (PARTITION BY customer_id ORDER BY last_known_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance_dense_rank,
    SUM(balance_dense_rank) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance_dense_rank
FROM enriched_accounts;
