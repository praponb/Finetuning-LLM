WITH latest_match AS (
    SELECT
        s.id,
        s.name,
        s.email,
        s.country,
        s.segment,
        s.signup_date,
        s.is_active,
        s.credit_limit,
        s.age,
        top1.order_amount,
        top1.discount
    FROM customers s
    JOIN LATERAL (
            SELECT c.amount AS order_amount, c.discount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    CASE WHEN discount >= 0.33 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_tier,
    COUNT(*) FILTER (WHERE country = 'JP') AS jp_rows,
    COUNT(DISTINCT country) AS distinct_countries,
    AVG(age) AS avg_age
FROM latest_match
GROUP BY ROLLUP (DATE_TRUNC('MONTH', signup_date), CASE WHEN discount >= 0.33 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING COUNT(*) FILTER (WHERE country = 'JP') < 1000
ORDER BY activity_month DESC NULLS LAST;
