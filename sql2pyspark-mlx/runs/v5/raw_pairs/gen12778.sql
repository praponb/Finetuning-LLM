WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < '2023-08-01'
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.email,
        b.name,
        b.country
    FROM kept e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_type,
        opened_date,
        status,
        email,
        country,
        AVG(balance) OVER (PARTITION BY country ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS balance_avg_5,
        LAG(balance, 2) OVER (PARTITION BY account_type ORDER BY opened_date, id) AS prev_balance,
        MIN(balance) OVER (PARTITION BY status) AS status_min_balance,
        MAX(balance) OVER (PARTITION BY country) AS country_max_balance,
        NTILE(3) OVER (PARTITION BY country ORDER BY balance DESC, id) AS balance_bucket
    FROM enriched
    WHERE balance BETWEEN 51000 AND 57000
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        prev_balance,
        COUNT(balance_bucket) OVER w_running AS running_count_balance_bucket,
        SUM(balance_bucket) OVER w_running AS running_sum_balance_bucket,
        MAX(balance_bucket) OVER w_running AS running_max_balance_bucket
    FROM with_windows
    WINDOW w_running AS (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.amount,
        top1.transaction_txn_type
    FROM scored s
    JOIN LATERAL (
            SELECT c.amount, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_txn_type ORDER BY running_max_balance_bucket DESC NULLS LAST, id) AS row_num
        FROM with_latest
    ) t
    WHERE row_num = 1
)
-- Grouped metrics
SELECT
    transaction_txn_type,
    CASE WHEN running_sum_balance_bucket >= 34 THEN 'HIGH' ELSE 'LOW' END AS running_sum_balance_bucket_tier,
    ROUND(AVG(prev_balance), 2) AS avg_prev_balance,
    SIZE(COLLECT_SET(transaction_txn_type)) AS num_transaction_txn_types,
    SORT_ARRAY(COLLECT_SET(transaction_txn_type)) AS transaction_txn_type_list,
    MAX(running_sum_balance_bucket) FILTER (WHERE transaction_txn_type = 'DEBIT') AS debit_max_running_sum_balance_bucket,
    SORT_ARRAY(COLLECT_SET(transaction_txn_type)) AS transaction_txn_type_list_2,
    ROUND(AVG(prev_balance), 2) AS avg_prev_balance_2
FROM ranked
GROUP BY CUBE (transaction_txn_type, CASE WHEN running_sum_balance_bucket >= 34 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY num_transaction_txn_types DESC NULLS FIRST;
