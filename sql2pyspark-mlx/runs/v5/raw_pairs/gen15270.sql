WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        country,
        segment,
        is_active,
        credit_limit,
        age,
        TRIM(country) AS clean_country,
        COALESCE(city, 'DIRECT') AS city_clean
    FROM customers
    WHERE segment IS NOT NULL
      AND segment != 'SMB'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment, country ORDER BY credit_limit DESC NULLS LAST, id) = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.plan = 'standard'
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(age) OVER (ORDER BY age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS age_avg_6,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS row_seq
    FROM kept
    WHERE segment IS NOT NULL
),
kept_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.amount <= 600
        )
)
-- Grouped metrics
SELECT
    CASE WHEN row_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_tier,
    APPROX_PERCENTILE(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles,
    MAX(age) AS max_age,
    SUM(age) FILTER (WHERE segment = 'Corporate') AS corporate_total_age,
    AVG(credit_limit) FILTER (WHERE country = 'US') AS us_avg_credit_limit,
    ROUND(SUM(age_avg_6) / NULLIF(COUNT(CASE WHEN segment = 'SMB' THEN 1 END), 0), 2) AS age_avg_6_per_smb,
    ROUND(SUM(row_seq) / NULLIF(COUNT(CASE WHEN segment = 'Consumer' THEN 1 END), 0), 2) AS row_seq_per_consumer
FROM kept_customers
GROUP BY row_seq_tier;
