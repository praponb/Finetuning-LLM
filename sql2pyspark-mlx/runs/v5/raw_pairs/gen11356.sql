WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) li AS element
),
with_dims AS (
    SELECT
        s.id,
        s.customer_id,
        d.credit_limit
    FROM flattened s
    INNER JOIN customers d
        ON s.customer_id = d.id
    WHERE d.name LIKE '%Diaz%'
),
valid AS (
    SELECT *, ROUND(credit_limit * 0.9, 2) AS credit_limit_adjusted
    FROM with_dims
    WHERE credit_limit >= 14800
),
enriched AS (
    SELECT
        src.*,
        c.segment,
        c.credit_limit AS customer_credit_limit,
        c.signup_date
    FROM valid src
    LEFT JOIN customers c
        ON src.customer_id = c.id
)
SELECT
    customer_id,
    SUM(customer_credit_limit) FILTER (WHERE segment = 'Consumer') AS consumer_total_customer_credit_limit,
    COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count,
    COUNT(CASE WHEN segment = 'SMB' THEN 1 END) AS smb_count,
    SUM(customer_credit_limit) AS total_customer_credit_limit
FROM enriched
GROUP BY customer_id
HAVING COUNT(CASE WHEN segment = 'SMB' THEN 1 END) > 1
ORDER BY customer_id DESC;
