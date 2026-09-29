WITH matched_keys AS (
    SELECT segment
    FROM customers
    INTERSECT
    SELECT segment
    FROM customers
    WHERE credit_limit <= 7400
),
reconciled AS (
    SELECT s.*
    FROM customers s
    INNER JOIN matched_keys k ON s.segment = k.segment
)
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    SUM(credit_limit) FILTER (WHERE country = 'JP') AS jp_total_credit_limit,
    CORR(age, credit_limit) AS corr_age_credit_limit,
    STDDEV_SAMP(age) AS age_stddev,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY CORR(age, credit_limit) DESC) AS corr_age_credit_limit_percent_rank
FROM reconciled
GROUP BY activity_month, quarter_start, year_start
