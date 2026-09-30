WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label,
        COALESCE(status, 'NONE') AS status_clean
    FROM accounts
    WHERE opened_date >= DATE '2023-06-25'
),
cleaned_accounts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        status_clean,
        CASE WHEN balance >= 76000 THEN 'High' WHEN balance >= 19000 THEN 'Medium' ELSE 'Low' END AS balance_level,
        COALESCE(status, 'UNKNOWN') AS status_clean_2
    FROM cleaned
    WHERE opened_date > TO_DATE('2023-09-30')
      AND customer_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_balance,
        AVG(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_balance,
        MAX(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_balance
    FROM cleaned_accounts
    WHERE account_type NOT IN ('CREDIT', 'SAVINGS', 'CHECKING')
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.credit_limit,
        d.signup_date
    FROM metrics s
    INNER JOIN customers d
        ON s.customer_id <=> d.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.opened_date,
        src.status,
        src.status_clean,
        src.balance_level,
        src.status_clean_2,
        src.running_max_balance,
        src.credit_limit,
        p.age,
        p.city
    FROM with_dims src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= s.opened_date
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN age >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    status,
    COUNT(DISTINCT city) AS distinct_cities,
    COUNT(*) FILTER (WHERE status = 'CLOSED') AS closed_rows,
    APPROX_PERCENTILE(running_max_balance, 0.95) AS running_max_balance_p95
FROM eligible
GROUP BY CASE WHEN age >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END, status
