WITH cleaned AS (
    SELECT
        *,
        ROUND(balance, 1) AS balance_rounded,
        COALESCE(status, 'N/A') AS status_clean
    FROM accounts
    WHERE opened_date >= DATE '2023-06-03'
),
enriched AS (
    SELECT e.*, c.age
    FROM cleaned e
    LEFT JOIN customers c
        ON e.customer_id = c.id
    WHERE NOT c.is_active
),
enriched_accounts AS (
    SELECT
        a.*,
        p.city,
        p.segment
    FROM enriched a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
with_window AS (
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM enriched_accounts src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.opened_date >= sub.start_date
       AND src.opened_date <= DATE_ADD(sub.end_date, 3)
)
SELECT
    CASE WHEN balance >= 87000 THEN 'LARGE' ELSE 'SMALL' END AS balance_tier,
    customer_id,
    COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) AS closed_count,
    COUNT(DISTINCT city) AS distinct_cities,
    MAX(balance) AS max_balance,
    ROUND(SUM(subscription_id) / NULLIF(COUNT(CASE WHEN segment = 'Consumer' THEN 1 END), 0), 2) AS subscription_id_per_consumer,
    ROUND(AVG(subscription_id), 2) AS avg_subscription_id,
    ROUND(SUM(balance_rounded) / NULLIF(COUNT(CASE WHEN account_type = 'LOAN' THEN 1 END), 0), 2) AS balance_rounded_per_loan
FROM with_window
GROUP BY CASE WHEN balance >= 87000 THEN 'LARGE' ELSE 'SMALL' END, customer_id
ORDER BY balance_rounded_per_loan DESC NULLS LAST;
