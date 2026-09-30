WITH with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        s.balance,
        s.opened_date,
        s.status,
        x.amount
    FROM accounts s
    LEFT JOIN LATERAL (
            SELECT c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(5) OVER w_ordered AS amount_bucket,
        NTILE(10) OVER (PARTITION BY status ORDER BY balance ASC, id ASC) AS balance_bucket,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_type, status), 4) AS amount_share
    FROM with_latest
    WHERE opened_date <= '2023-10-05'
    WINDOW w_ordered AS (PARTITION BY status, account_type ORDER BY amount ASC, id ASC)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_SUB(opened_date, 62) AS opened_date_minus_62d
    FROM metrics
    WHERE opened_date >= DATE '2023-09-07'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.amount BETWEEN 2200 AND 3500
        )
)
SELECT
    id,
    balance,
    opened_date,
    status,
    amount,
    amount_bucket,
    balance_bucket,
    amount_share,
    opened_date_minus_62d,
    CASE WHEN status RLIKE '^(FROZEN|CLOSED)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS status_zone
FROM kept
ORDER BY status ASC NULLS LAST
