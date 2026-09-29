WITH with_dims AS (
    SELECT
        src.id,
        src.account_id,
        src.txn_type,
        src.txn_time,
        src.is_flagged,
        d.balance
    FROM transactions src
    LEFT JOIN accounts d
        ON src.account_id = d.id
    WHERE d.opened_date BETWEEN '2024-05-12' AND '2024-11-17'
),
joined AS (
    SELECT
        src.*,
        a.opened_date,
        a.account_type
    FROM with_dims src
    LEFT JOIN accounts a
        ON src.account_id = a.id
),
valid AS (
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        is_flagged,
        balance,
        opened_date,
        LOWER(account_type) AS account_type_lower
    FROM joined
    WHERE txn_time > DATE '2023-07-02'
),
with_dims_transactions AS (
    SELECT
        e.id,
        e.account_id,
        e.is_flagged,
        e.balance,
        e.account_type_lower,
        d.balance AS account_balance
    FROM valid e
    LEFT JOIN accounts d
        ON e.account_id = d.id
),
enriched AS (
    SELECT
        src.*,
        b.balance AS account_balance_2,
        b.opened_date
    FROM with_dims_transactions src
    INNER JOIN accounts b
        ON src.account_id = b.id
),
with_windows AS (
    SELECT
        *,
        LEAD(account_balance_2, 1) OVER w_ordered AS next_account_balance_2,
        PERCENT_RANK() OVER w_ordered_2 AS account_balance_percent_rank,
        FIRST_VALUE(account_balance) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY account_balance_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_account_balance,
        NTILE(3) OVER (ORDER BY account_balance_2, id) AS account_balance_2_bucket
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY account_id ORDER BY account_balance, id), w_ordered_2 AS (PARTITION BY account_id ORDER BY account_balance DESC)
)
SELECT
    dim.status,
    dim.account_type,
    COUNT(*) AS num_rows,
    MAX(s.balance) AS max_balance,
    GROUPING_ID() AS grouping_level
FROM with_windows s
LEFT JOIN accounts dim ON s.account_id = dim.id
GROUP BY GROUPING SETS ((dim.status, dim.account_type), (dim.status), ());
