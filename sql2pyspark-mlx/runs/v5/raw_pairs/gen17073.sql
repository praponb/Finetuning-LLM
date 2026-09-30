WITH matched_keys AS (
    SELECT id, segment
    FROM customers
    INTERSECT ALL
    SELECT id, segment
    FROM customers
    WHERE city = 'Tokyo'
),
reconciled AS (
    SELECT s.*
    FROM customers s
    INNER JOIN matched_keys k ON s.id = k.id AND s.segment = k.segment
)
SELECT
    DATE_TRUNC('WEEK', signup_date) AS week_start,
    CASE WHEN age >= 63 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
    STDDEV_SAMP(credit_limit) AS credit_limit_stddev,
    COUNT(*) FILTER (WHERE country = 'TH') AS th_rows,
    SUM(CASE WHEN country = 'SG' THEN age ELSE 0 END) AS sg_age,
    ROUND(SUM(age) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS age_per_chiang_mai
FROM reconciled
GROUP BY week_start, age_tier
ORDER BY th_rows DESC NULLS LAST;
