WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY account_type, status ORDER BY opened_date DESC) AS dedup_rank
    FROM accounts
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        dedup_rank,
        AVG(balance) OVER (PARTITION BY status, account_type) AS status_avg_balance,
        AVG(balance) OVER (ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS balance_avg_6_prior,
        PERCENT_RANK() OVER (ORDER BY balance NULLS FIRST) AS balance_percent_rank,
        SUM(balance) OVER (PARTITION BY account_type, status ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance
    FROM latest
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.opened_date,
    s.status,
    s.dedup_rank,
    s.status_avg_balance,
    CASE WHEN account_type RLIKE '^(CHECKING|CREDIT)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS account_type_zone,
    (SELECT SUM(x.discount) FROM orders x WHERE x.customer_id = s.customer_id) AS sum_order_discount
FROM kept s
