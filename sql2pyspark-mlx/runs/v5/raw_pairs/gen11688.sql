WITH kept_keys AS (
    SELECT status, account_type
    FROM accounts
    EXCEPT
    SELECT status, account_type
    FROM accounts
    WHERE customer_id IS NOT NULL
),
reconciled AS (
    SELECT s.*
    FROM accounts s
    JOIN kept_keys k ON s.status = k.status AND s.account_type = k.account_type
),
eligible AS (
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND HOUR(x.txn_time) BETWEEN 9 AND 12
        )
),
with_dims AS (
    SELECT s.*, d.credit_limit
    FROM eligible s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
filtered AS (
    SELECT
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        credit_limit,
        LENGTH(account_type) AS account_type_length,
        COALESCE(status, 'N/A') AS resolved_status
    FROM with_dims
    WHERE status IS NOT NULL
),
with_dims_accounts AS (
    SELECT
        src.customer_id,
        src.account_type,
        src.balance,
        src.opened_date,
        src.resolved_status,
        c.age,
        c.name
    FROM filtered src
    LEFT JOIN customers c
        ON src.customer_id = c.id
    WHERE c.city = 'Bangkok'
),
grouped AS (
    SELECT
        CASE WHEN age >= 63 THEN 'LARGE' ELSE 'SMALL' END AS age_tier,
        DATE_TRUNC('WEEK', opened_date) AS week_start,
        ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN account_type = 'LOAN' THEN 1 END), 0), 2) AS balance_per_loan,
        STDDEV_SAMP(balance) AS balance_stddev,
        SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
        ROUND(AVG(balance), 2) AS avg_balance
    FROM with_dims_accounts
    GROUP BY CASE WHEN age >= 63 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', opened_date)
    HAVING balance_per_loan >= 0
)
SELECT
    age_tier,
    week_start,
    balance_per_loan,
    balance_stddev,
    account_type_list,
    avg_balance
FROM grouped
