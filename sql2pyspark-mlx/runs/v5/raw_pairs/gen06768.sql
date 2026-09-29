WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        CAST(balance AS INT) AS balance_int
    FROM accounts
    WHERE opened_date >= TO_DATE('2023-09-13')
      AND customer_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(balance) OVER w_grp AS running_balance,
        SUM(balance) OVER (ORDER BY balance_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance_2,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS balance_avg_3_prior
    FROM filtered
    WINDOW w_grp AS (PARTITION BY customer_id ORDER BY balance_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(running_balance) OVER w_ordered AS next_running_balance,
        LAG(balance) OVER w_ordered AS prev_balance,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM with_windows
    WHERE running_balance < 120162
    WINDOW w_ordered AS (PARTITION BY account_type ORDER BY balance, id)
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM enriched src
    INNER JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.opened_date >= sub.start_date
       AND src.opened_date < sub.end_date + INTERVAL 30 DAYS
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY row_seq DESC, id, subscription_id) AS row_num
    FROM attributed
    QUALIFY row_num = 1
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.transaction_txn_time,
        latest.amount
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Join a dimension and aggregate
SELECT
    p.city,
    p.country,
    src.plan,
    COUNT(*) AS num_rows,
    MAX(src.subscription_id) AS max_subscription_id,
    GROUPING_ID() AS gid
FROM latest_match src
INNER JOIN customers p ON src.customer_id = p.id
GROUP BY ROLLUP (p.city, p.country, src.plan)
ORDER BY num_rows DESC NULLS FIRST
