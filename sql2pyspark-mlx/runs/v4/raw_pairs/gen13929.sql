WITH enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.segment,
        d.city,
        d.credit_limit
    FROM subscriptions s
    LEFT JOIN customers d
        ON s.customer_id = d.id
)
-- Multi-level aggregation
SELECT
    plan,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SUM(CASE WHEN plan = 'standard' THEN monthly_fee ELSE 0 END) AS standard_monthly_fee,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit_2
FROM enriched
GROUP BY plan
ORDER BY num_cities DESC;
