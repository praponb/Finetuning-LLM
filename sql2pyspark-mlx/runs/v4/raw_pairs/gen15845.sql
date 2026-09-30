SELECT
    DATE_TRUNC('WEEK', signup_date) AS week_start,
    CASE WHEN age >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    STDDEV_SAMP(age) AS age_stddev,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', signup_date) ORDER BY STDDEV_SAMP(age) DESC) AS age_stddev_rank
FROM customers
GROUP BY week_start, age_tier
ORDER BY week_start NULLS FIRST;
