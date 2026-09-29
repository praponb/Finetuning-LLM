WITH kept_keys AS (
    -- Set difference on keys
    SELECT segment, id
    FROM customers
    EXCEPT DISTINCT
    SELECT segment, id
    FROM customers
    WHERE signup_date < '2023-03-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM customers s
    INNER JOIN kept_keys k ON s.segment = k.segment AND s.id = k.id
)
-- Grouped metrics
SELECT
    CASE WHEN age >= 65 THEN 'LARGE' ELSE 'SMALL' END AS age_tier,
    APPROX_PERCENTILE(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    APPROX_PERCENTILE(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles_2,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    COUNT(*) AS cnt,
    COUNT_IF(country = 'SG') AS sg_events,
    AVG(credit_limit) AS avg_credit_limit_2
FROM verified
GROUP BY ROLLUP (CASE WHEN age >= 65 THEN 'LARGE' ELSE 'SMALL' END)
HAVING AVG(credit_limit) < 1000000
