WITH with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.credit_limit,
        p.segment,
        p.age
    FROM sessions e
    LEFT JOIN customers p
        ON e.customer_id = p.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN credit_limit >= 17500 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
    APPROX_PERCENTILE(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles,
    SUM(credit_limit) AS total_credit_limit,
    SUM(CASE WHEN country = 'JP' THEN credit_limit ELSE 0 END) AS jp_credit_limit
FROM with_dims
GROUP BY CASE WHEN credit_limit >= 17500 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY jp_credit_limit NULLS LAST;
