WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', account_type, status) AS account_type_status_key,
        COALESCE(status, 'UNKNOWN') AS resolved_status
    FROM accounts
    WHERE YEAR(opened_date) >= 2023
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        account_type_status_key,
        resolved_status,
        FIRST_VALUE(status) IGNORE NULLS OVER (PARTITION BY account_type ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status,
        ROW_NUMBER() OVER w_grp AS seq_no,
        SUM(balance) OVER (PARTITION BY customer_id ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance,
        AVG(balance) OVER (PARTITION BY status ORDER BY opened_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS balance_avg_2
    FROM valid
    WINDOW w_grp AS (PARTITION BY account_type ORDER BY balance DESC, id ASC)
),
with_dims AS (
    -- Attach customers attributes
    SELECT e.*, b.credit_limit
    FROM metrics e
    INNER JOIN customers b
        ON e.customer_id = b.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM with_dims
    EXCEPT ALL
    SELECT status, id
    FROM accounts
    WHERE status NOT IN ('OPEN', 'FROZEN', 'CLOSED')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.id = k.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN balance >= 67000 THEN 'LARGE' ELSE 'SMALL' END AS balance_tier,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    SIZE(COLLECT_SET(account_type)) AS num_account_types,
    APPROX_PERCENTILE(cumulative_balance, 0.5) AS cumulative_balance_p50,
    ROUND(AVG(balance), 2) AS avg_balance,
    APPROX_PERCENTILE(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles
FROM kept_rows
GROUP BY CASE WHEN balance >= 67000 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', opened_date)
ORDER BY balance_tier ASC NULLS LAST;
