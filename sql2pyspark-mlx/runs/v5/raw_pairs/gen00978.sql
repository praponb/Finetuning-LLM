WITH matched_keys AS (
    SELECT city, country
    FROM customers
    EXCEPT
    SELECT city, country
    FROM customers
    WHERE segment = 'SMB'
),
kept_rows AS (
    SELECT s.*
    FROM customers s
    INNER JOIN matched_keys k ON s.city = k.city AND s.country = k.country
),
with_windows AS (
    SELECT
        id,
        name,
        email,
        credit_limit,
        age,
        CORR(credit_limit, age) OVER (PARTITION BY city) AS corr_credit_limit_age,
        DENSE_RANK() OVER (ORDER BY age) AS age_dense_rank
    FROM kept_rows
),
metrics AS (
    SELECT
        id,
        name,
        email,
        age_dense_rank,
        DENSE_RANK() OVER (ORDER BY age_dense_rank DESC) AS age_dense_rank_dense_rank,
        PERCENT_RANK() OVER (ORDER BY corr_credit_limit_age DESC NULLS FIRST) AS corr_credit_limit_age_percent_rank,
        RANK() OVER (ORDER BY age_dense_rank) AS age_dense_rank_rank
    FROM with_windows
),
kept AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.status IN ('CANCELLED', 'PENDING')
        )
),
kept_keys AS (
    SELECT id
    FROM kept
    EXCEPT DISTINCT
    SELECT id
    FROM customers
    WHERE segment IS NULL
),
reconciled AS (
    SELECT s.*
    FROM kept s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    id,
    name,
    age_dense_rank,
    age_dense_rank_dense_rank,
    age_dense_rank_rank,
    CASE WHEN name RLIKE '^(Mia Rossi|Kanya Srisuk)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS name_zone
FROM reconciled
ORDER BY age_dense_rank_rank NULLS LAST
