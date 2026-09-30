WITH base AS (
    SELECT
        *,
        ROUND(balance * 0.9, 2) AS adjusted_balance,
        CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END AS is_open
    FROM accounts
    WHERE balance BETWEEN 56000 AND 59000
),
with_windows AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        adjusted_balance,
        is_open,
        SUM(balance) OVER (PARTITION BY account_type ORDER BY adjusted_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        RANK() OVER (PARTITION BY account_type ORDER BY is_open DESC) AS is_open_rank
    FROM base
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) AS rn
    FROM with_windows
    QUALIFY rn = 1
),
kept_keys AS (
    SELECT status, customer_id
    FROM latest
    EXCEPT
    SELECT status, customer_id
    FROM accounts
    WHERE status IN ('CLOSED', 'OPEN', 'FROZEN')
),
kept_rows AS (
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.status = k.status AND s.customer_id = k.customer_id
),
enriched AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY is_open) AS is_open_cume_dist,
        RANK() OVER w_hist AS running_balance_rank,
        COUNT(running_balance) OVER w_recent AS customer_id_count_running_balance,
        CUME_DIST() OVER (PARTITION BY account_type ORDER BY balance DESC NULLS FIRST) AS balance_cume_dist,
        DENSE_RANK() OVER w_ordered AS balance_dense_rank
    FROM kept_rows
    WINDOW w_hist AS (PARTITION BY status ORDER BY running_balance DESC NULLS LAST), w_recent AS (PARTITION BY customer_id), w_ordered AS (ORDER BY balance NULLS LAST)
)
SELECT
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    running_balance,
    is_open_rank,
    rn,
    running_balance_rank,
    CASE WHEN status RLIKE '^(CLOSED|FROZEN)$' THEN 'GROUP_A' WHEN status RLIKE '^(OPEN)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS status_zone
FROM enriched
ORDER BY is_open_rank DESC NULLS FIRST;
