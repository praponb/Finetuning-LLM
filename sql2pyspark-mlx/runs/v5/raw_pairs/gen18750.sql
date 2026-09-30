WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_SUB(order_date, 83) AS order_date_minus_83d
    FROM orders
    WHERE channel IS NOT NULL
      AND channel IS NOT DISTINCT FROM 'partner'
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.segment,
        p.email
    FROM filtered e
    INNER JOIN customers p
        ON e.customer_id = p.id
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM joined a
    INNER JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.order_date BETWEEN sub.start_date AND sub.end_date
),
latest_match AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.payment_amount,
        x.payment_paid_at
    FROM with_window s
    LEFT JOIN LATERAL (
            SELECT c.amount AS payment_amount, c.paid_at AS payment_paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN monthly_fee >= 191 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_tier,
    COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count,
    COUNT(DISTINCT status) AS distinct_status
FROM latest_match
GROUP BY CASE WHEN monthly_fee >= 191 THEN 'LARGE' ELSE 'SMALL' END
HAVING distinct_status > 1;
