WITH remaining_keys AS (
    SELECT id, status
    FROM accounts
    INTERSECT ALL
    SELECT id, status
    FROM accounts
    WHERE balance < 72000
),
reconciled AS (
    SELECT s.*
    FROM accounts s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.status = k.status
),
filtered AS (
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        ABS(balance) abs_balance,
        COALESCE(balance, 0) balance_filled
    FROM reconciled
    WHERE customer_id IS NOT NULL
),
not_flagged AS (
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= s.opened_date
        )
),
joined AS (
    SELECT
        src.*,
        d.email,
        d.city,
        d.signup_date
    FROM not_flagged src
    LEFT JOIN customers d
        ON src.customer_id = d.id
)
SELECT
    id,
    customer_id,
    opened_date,
    abs_balance,
    balance_filled,
    email,
    city,
    CASE WHEN city RLIKE '^(Berlin|Singapore)$' THEN 'GROUP_A' ELSE 'CORE' END AS city_zone
FROM joined;
