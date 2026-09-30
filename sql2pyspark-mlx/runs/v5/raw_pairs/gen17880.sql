WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(balance, 1) OVER (PARTITION BY account_type, customer_id ORDER BY balance, id) AS next_balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) AS seq_no,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY balance NULLS LAST) AS balance_percent_rank
    FROM accounts
    WHERE balance > 73000
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        opened_date,
        status,
        next_balance,
        balance_percent_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY opened_date NULLS LAST, id) AS row_num
        FROM metrics
    ) t
    WHERE row_num = 1
),
metrics_accounts AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status,
        balance_percent_rank,
        MAX(balance_percent_rank) OVER (PARTITION BY customer_id, status) AS customer_id_max_balance_percent_rank,
        AVG(next_balance) OVER (ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS next_balance_avg_3,
        SUM(next_balance) OVER w_hist AS running_next_balance,
        LEAD(next_balance, 2) OVER (PARTITION BY status ORDER BY opened_date, id) AS next_next_balance,
        FIRST_VALUE(status) IGNORE NULLS OVER w_hist AS first_known_status
    FROM ranked
    WINDOW w_hist AS (PARTITION BY status ORDER BY balance_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(status, TRUE) OVER w_hist AS first_known_status_2,
        NTILE(10) OVER w_grp AS customer_id_max_balance_percent_rank_bucket,
        SUM(running_next_balance) OVER w_hist AS running_running_next_balance,
        NTILE(10) OVER w_part AS customer_id_max_balance_percent_rank_bucket_2,
        ROW_NUMBER() OVER w_part AS seq_no
    FROM metrics_accounts
    WINDOW w_hist AS (PARTITION BY status ORDER BY customer_id_max_balance_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY status ORDER BY customer_id_max_balance_percent_rank, id), w_part AS (PARTITION BY status ORDER BY customer_id_max_balance_percent_rank DESC, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.account_id IN (4, 20, 37)
        )
)
SELECT
    id,
    status,
    balance_percent_rank,
    next_next_balance,
    customer_id_max_balance_percent_rank_bucket,
    running_running_next_balance,
    customer_id_max_balance_percent_rank_bucket_2,
    CASE WHEN status RLIKE '^(FROZEN|CLOSED)$' THEN 'GROUP_A' WHEN status RLIKE '^(OPEN)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS status_zone
FROM not_flagged;
