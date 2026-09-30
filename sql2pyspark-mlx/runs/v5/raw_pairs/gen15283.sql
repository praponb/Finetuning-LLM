WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY signup_date DESC NULLS LAST, id) AS dedup_rank
    FROM customers
    QUALIFY dedup_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN age >= 63 THEN 'LARGE' ELSE 'SMALL' END AS age_tier,
    ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN city = 'Bangkok' THEN 1 END), 0), 2) AS age_per_bangkok,
    ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN segment = 'Corporate' THEN 1 END), 0), 2) AS age_per_corporate,
    CORR(age, credit_limit) AS corr_age_credit_limit,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit
FROM latest
GROUP BY 1
ORDER BY age_per_bangkok;
