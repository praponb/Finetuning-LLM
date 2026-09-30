WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        balance,
        status,
        LEAD(balance, 1) OVER (PARTITION BY status, account_type ORDER BY opened_date, id) AS next_balance,
        FIRST_VALUE(status, TRUE) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status
    FROM accounts
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_type = 'DEBIT'
        )
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        balance,
        status,
        next_balance,
        first_known_status,
        SUBSTRING(status, 1, 3) AS status_prefix
    FROM not_flagged
    WHERE balance BETWEEN 44000 AND 64000
      AND status IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.status,
        e.next_balance,
        e.first_known_status,
        b.name
    FROM filtered e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY next_balance DESC NULLS FIRST, id) AS latest_rank
        FROM with_dims
    ) x
    WHERE latest_rank = 1
)
SELECT
    s.id,
    s.customer_id,
    s.status,
    s.first_known_status,
    s.name,
    CASE WHEN status RLIKE '^(FROZEN|CLOSED)$' THEN 'GROUP_A' WHEN status RLIKE '^(OPEN)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS status_zone,
    (SELECT MIN(o2.amount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS min_order_amount
FROM latest s
ORDER BY status DESC, status_zone;
