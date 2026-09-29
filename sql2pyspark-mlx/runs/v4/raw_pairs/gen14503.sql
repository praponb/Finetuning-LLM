WITH with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.credit_limit,
        p.city,
        p.email
    FROM accounts s
    INNER JOIN customers p
        ON s.customer_id = p.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT status, account_type
    FROM with_dims
    EXCEPT DISTINCT
    SELECT status, account_type
    FROM accounts
    WHERE status IN ('FROZEN', 'CLOSED', 'OPEN')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.status = k.status AND s.account_type = k.account_type
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.merchant IN ('7-Eleven', 'Lazada', 'Amazon')
        )
),
daily AS (
    -- Multi-level aggregation
    SELECT
        customer_id,
        DATE_TRUNC('WEEK', opened_date) AS week_start,
        APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
        MAX(credit_limit) FILTER (WHERE status = 'FROZEN') AS frozen_max_credit_limit,
        PERCENTILE_APPROX(balance, 0.5) AS balance_p50,
        ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN account_type = 'CHECKING' THEN 1 END), 0), 2) AS credit_limit_per_checking
    FROM kept
    GROUP BY ROLLUP (customer_id, DATE_TRUNC('WEEK', opened_date))
    HAVING credit_limit_per_checking > 0
)
SELECT
    s.customer_id,
    s.frozen_max_credit_limit,
    s.balance_p50,
    s.credit_limit_per_checking,
    (SELECT MAX(o2.amount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS max_order_amount
FROM daily s
ORDER BY customer_id DESC, credit_limit_per_checking DESC
