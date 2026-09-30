WITH joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.age,
        d.segment
    FROM accounts s
    INNER JOIN customers d
        ON s.customer_id = d.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, customer_id
    FROM joined
    EXCEPT
    SELECT id, customer_id
    FROM accounts
    WHERE status IN ('OPEN', 'CLOSED', 'FROZEN')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.is_flagged
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < s.opened_date
        )
),
not_flagged_accounts AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time < s.opened_date
        )
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
        age,
        segment,
        RANK() OVER w_ordered AS age_rank,
        MIN(balance) OVER (PARTITION BY account_type) AS account_type_min_balance,
        MAX(balance) OVER w_grp AS account_type_max_balance,
        PERCENT_RANK() OVER w_running AS age_percent_rank
    FROM not_flagged_accounts
    WINDOW w_ordered AS (PARTITION BY status ORDER BY age DESC NULLS FIRST), w_grp AS (PARTITION BY account_type, status), w_running AS (ORDER BY age)
)
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    segment,
    age_rank,
    account_type_min_balance,
    CASE WHEN account_type RLIKE '^(CHECKING|CREDIT)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS account_type_zone
FROM metrics
ORDER BY age_rank, account_type
