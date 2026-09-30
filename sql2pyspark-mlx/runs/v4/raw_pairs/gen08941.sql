WITH enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.signup_date,
        d.email
    FROM subscriptions a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY monthly_fee, id) row_seq,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) monthly_fee_share,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) monthly_fee_avg_6
    FROM enriched
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.credit_limit,
        p.signup_date AS customer_signup_date
    FROM with_windows src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(email)) email_title
    FROM enriched_subscriptions
    WHERE start_date BETWEEN '2025-10-23' AND '2025-10-26'
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        status,
        monthly_fee,
        monthly_fee_share,
        credit_limit
    FROM valid
) src
UNPIVOT (
    value FOR metric IN (monthly_fee, monthly_fee_share, credit_limit)
);
