WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_SUB(opened_date, 56) AS opened_date_minus_56d,
        COALESCE(status, 'DIRECT') AS status_filled
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND YEAR(opened_date) = 2024
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY account_type ORDER BY opened_date_minus_56d DESC) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.name,
        c.signup_date
    FROM deduped a
    INNER JOIN customers c
        ON a.customer_id = c.id
)
-- Grouped metrics
SELECT
    CASE WHEN balance >= 79000 THEN 'HIGH' ELSE 'LOW' END AS balance_tier,
    DATE_TRUNC('WEEK', signup_date) AS week_start,
    ROUND(AVG(balance), 2) AS avg_balance,
    COUNT(*) AS num_rows,
    ROUND(AVG(balance), 2) AS avg_balance_2,
    SIZE(COLLECT_SET(account_type)) AS num_account_types,
    COUNT(*) AS num_rows_2,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN balance >= 79000 THEN 'HIGH' ELSE 'LOW' END ORDER BY ROUND(AVG(balance), 2) DESC) AS avg_balance_2_dense_rank
FROM with_dims
GROUP BY CASE WHEN balance >= 79000 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', signup_date)
