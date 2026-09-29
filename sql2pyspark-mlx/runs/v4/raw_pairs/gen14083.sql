WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(balance) OVER w_recent AS balance_avg_2_prior,
        ROW_NUMBER() OVER w_recent_2 AS row_seq,
        RANK() OVER (PARTITION BY status ORDER BY balance NULLS FIRST) AS balance_rank,
        FIRST_VALUE(status) IGNORE NULLS OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status,
        NTILE(3) OVER (PARTITION BY account_type, status ORDER BY balance, id) AS balance_bucket
    FROM accounts
    WINDOW w_recent AS (ORDER BY balance, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_recent_2 AS (PARTITION BY status, customer_id ORDER BY balance, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_type = 'DEBIT'
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT account_type, customer_id
    FROM not_flagged
    EXCEPT DISTINCT
    SELECT account_type, customer_id
    FROM accounts
    WHERE status = 'CLOSED'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    INNER JOIN remaining_keys k ON s.account_type = k.account_type AND s.customer_id = k.customer_id
)
SELECT
    customer_id,
    account_type,
    balance,
    status,
    row_seq,
    balance_rank,
    first_known_status,
    balance_bucket,
    CASE WHEN account_type RLIKE '^(CREDIT|CHECKING)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS account_type_zone
FROM kept_rows
ORDER BY status DESC NULLS LAST;
