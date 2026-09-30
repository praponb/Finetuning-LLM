WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label
    FROM customers
    WHERE city IS DISTINCT FROM 'New York'
      AND country = 'TH'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN age >= 65 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
    APPROX_PERCENTILE(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN country = 'UK' THEN 1 END), 0), 2) AS age_per_uk,
    MIN(credit_limit) AS min_credit_limit
FROM valid
GROUP BY CASE WHEN age >= 65 THEN 'HIGH' ELSE 'LOW' END
