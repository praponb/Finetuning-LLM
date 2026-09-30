WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= s.opened_date
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY status ORDER BY balance DESC NULLS LAST) AS balance_rank,
        AVG(balance) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS balance_avg_2_prior,
        MAX(balance) OVER (PARTITION BY account_type, status) AS account_type_max_balance
    FROM not_flagged
),
kept_keys AS (
    -- Set difference on keys
    SELECT account_type, customer_id
    FROM metrics
    EXCEPT DISTINCT
    SELECT account_type, customer_id
    FROM accounts
    WHERE status IN ('FROZEN', 'OPEN')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN kept_keys k ON s.account_type = k.account_type AND s.customer_id = k.customer_id
),
joined AS (
    -- Attach customers attributes
    SELECT e.*, p.credit_limit
    FROM kept_rows e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT id
    FROM joined
    EXCEPT DISTINCT
    SELECT id
    FROM accounts
    WHERE MONTH(opened_date) IN (4, 6, 9)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN kept_keys_2 k ON s.id = k.id
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        account_type,
        COUNT_IF(status = 'CLOSED') AS closed_events,
        ROUND(AVG(balance_avg_2_prior), 2) AS avg_balance_avg_2_prior,
        COUNT(*) AS num_rows,
        COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS open_count,
        COUNT(CASE WHEN account_type = 'SAVINGS' THEN 1 END) AS savings_count
    FROM reconciled
    GROUP BY account_type
)
SELECT
    account_type,
    avg_balance_avg_2_prior,
    num_rows,
    open_count,
    savings_count,
    CASE WHEN account_type RLIKE '^(CHECKING|SAVINGS)$' THEN 'OTHER_ZONE' WHEN account_type RLIKE '^(CREDIT|LOAN)$' THEN 'EXTENDED' ELSE 'CORE' END AS account_type_zone
FROM daily
ORDER BY account_type_zone DESC NULLS LAST;
