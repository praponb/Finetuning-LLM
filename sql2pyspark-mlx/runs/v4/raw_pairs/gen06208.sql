WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM accounts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date NULLS LAST, id) = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN balance >= 62000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    ROUND(AVG(balance), 2) AS avg_balance,
    SUM(CASE WHEN status = 'CLOSED' THEN balance ELSE 0 END) AS closed_balance,
    COUNT(*) AS cnt,
    SIZE(COLLECT_SET(account_type)) AS num_account_types,
    APPROX_PERCENTILE(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles
FROM deduped
GROUP BY ROLLUP (CASE WHEN balance >= 62000 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', opened_date), DATE_TRUNC('WEEK', opened_date))
ORDER BY cnt
