WITH with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.country,
        c.signup_date,
        c.segment
    FROM accounts s
    LEFT JOIN customers c
        ON s.customer_id = c.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN balance >= 63000 THEN 'HIGH' ELSE 'LOW' END AS balance_tier,
    status,
    PERCENTILE_APPROX(balance, 0.9) AS balance_p90,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    MAX(balance) AS max_balance
FROM with_dims
GROUP BY CASE WHEN balance >= 63000 THEN 'HIGH' ELSE 'LOW' END, status
HAVING MAX(balance) > 0
ORDER BY balance_tier DESC NULLS FIRST;
