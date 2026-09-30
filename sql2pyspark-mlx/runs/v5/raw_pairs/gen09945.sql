WITH base AS (
    SELECT *, SUBSTRING(status, 1, 2) AS status_prefix
    FROM accounts
    WHERE opened_date > DATE '2023-03-20'
      AND opened_date <= '2023-09-01'
),
with_dims AS (
    SELECT s.*, d.city
    FROM base s
    LEFT JOIN customers d
        ON s.customer_id = d.id
    WHERE d.segment = 'Consumer'
),
matched AS (
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_dims a
    JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.opened_date BETWEEN sub.start_date AND sub.end_date
),
with_latest AS (
    SELECT
        s.id,
        s.customer_id,
        s.balance,
        s.opened_date,
        s.status,
        s.status_prefix,
        s.city,
        s.subscription_id,
        s.plan,
        s.monthly_fee,
        top1.merchant
    FROM matched s
    LEFT JOIN LATERAL (
            SELECT c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.merchant IN ('Starbucks', 'Amazon')
        )
),
valid AS (
    SELECT
        *,
        INITCAP(TRIM(plan)) AS plan_title,
        ROUND(monthly_fee, 0) AS monthly_fee_rounded
    FROM not_flagged
    WHERE opened_date > TO_DATE('2023-04-04')
)
SELECT
    s.id,
    s.customer_id,
    s.balance,
    s.opened_date,
    s.status,
    s.status_prefix,
    s.city,
    s.subscription_id,
    s.plan,
    (SELECT MAX(t2.amount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS max_order_amount
FROM valid s
ORDER BY subscription_id DESC
