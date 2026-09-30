WITH kept_keys AS (
    SELECT customer_id, status
    FROM accounts
    INTERSECT ALL
    SELECT customer_id, status
    FROM accounts
    WHERE customer_id IS NOT NULL
),
kept_rows AS (
    SELECT s.*
    FROM accounts s
    JOIN kept_keys k ON s.customer_id = k.customer_id AND s.status = k.status
),
joined AS (
    SELECT
        a.customer_id,
        a.account_type,
        a.balance,
        a.status,
        c.email,
        c.age,
        c.credit_limit
    FROM kept_rows a
    JOIN customers c
        ON a.customer_id <=> c.id
),
scored AS (
    SELECT
        account_type,
        balance,
        status,
        email,
        age,
        ROUND(age / SUM(age) OVER (PARTITION BY status), 4) AS age_share
    FROM joined
    WHERE credit_limit >= 13700
),
scored_accounts AS (
    SELECT
        *,
        MIN(balance) OVER (PARTITION BY status, account_type) AS status_min_balance,
        DENSE_RANK() OVER (ORDER BY balance DESC) AS balance_dense_rank,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY age DESC NULLS LAST) AS age_dense_rank,
        RANK() OVER (ORDER BY age_share NULLS LAST) AS age_share_rank,
        COUNT(age) OVER (PARTITION BY account_type) AS account_type_count_age
    FROM scored
),
filtered AS (
    SELECT
        *,
        LEAST(age, balance_dense_rank) AS least_age_balance_dense_rank,
        COALESCE(status, 'OTHER') AS status_clean
    FROM scored_accounts
    WHERE status IS NOT NULL
),
grouped AS (
    SELECT
        CASE WHEN account_type_count_age >= 16 THEN 'HIGH' ELSE 'LOW' END AS account_type_count_age_tier,
        CASE WHEN balance_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS balance_dense_rank_tier,
        SIZE(COLLECT_SET(status)) AS num_status,
        APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
        ROUND(AVG(balance), 2) AS avg_balance,
        ROUND(AVG(balance_dense_rank), 2) AS avg_balance_dense_rank,
        ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN status = 'CLOSED' THEN 1 END), 0), 2) AS balance_per_closed,
        SUM(CASE WHEN status = 'OPEN' THEN status_min_balance ELSE 0 END) AS open_status_min_balance,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN account_type_count_age >= 16 THEN 'HIGH' ELSE 'LOW' END ORDER BY SUM(CASE WHEN status = 'OPEN' THEN status_min_balance ELSE 0 END) DESC) AS open_status_min_balance_percent_rank
    FROM filtered
    GROUP BY 1, 2
)
SELECT
    account_type_count_age_tier,
    balance_dense_rank_tier,
    num_status,
    balance_quartiles,
    avg_balance,
    avg_balance_dense_rank,
    balance_per_closed,
    open_status_min_balance,
    open_status_min_balance_percent_rank
FROM grouped;
