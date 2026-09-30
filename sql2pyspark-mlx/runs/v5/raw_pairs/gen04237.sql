WITH eligible AS (
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date >= s.signup_date
        )
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS rn
        FROM eligible
    ) x
    WHERE rn = 1
),
not_flagged AS (
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.status = 'ACTIVE'
        )
),
latest AS (
    SELECT *
    FROM not_flagged
    QUALIFY RANK() OVER (PARTITION BY city, country ORDER BY signup_date) = 1
),
metrics AS (
    SELECT
        *,
        STDDEV_SAMP(credit_limit) OVER (PARTITION BY city) AS city_stddev_samp_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rn DESC, id) AS row_seq
    FROM latest
)
SELECT
    CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    COUNT(*) FILTER (WHERE country = 'DE') AS de_rows,
    ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS credit_limit_per_chiang_mai
FROM metrics
GROUP BY CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', signup_date);
