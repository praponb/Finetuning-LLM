WITH cleaned AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        LAST_DAY(start_date) start_date_month_end
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND start_date > '2024-01-11'
),
with_dims AS (
    SELECT
        a.*,
        c.city,
        c.name,
        c.segment
    FROM cleaned a
    INNER JOIN customers c
        ON a.customer_id = c.id
),
enriched AS (
    SELECT src.*, c.signup_date
    FROM with_dims src
    INNER JOIN customers c
        ON src.customer_id = c.id
),
remaining_keys AS (
    SELECT plan, id
    FROM enriched
    EXCEPT
    SELECT plan, id
    FROM subscriptions
    WHERE plan IN ('premium', 'standard', 'enterprise')
),
reconciled AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.plan = k.plan AND s.id = k.id
)
SELECT
    DATE_TRUNC('MONTH', start_date_month_end) AS activity_month,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    COUNT(*) AS event_count,
    SUM(CASE WHEN plan = 'enterprise' THEN monthly_fee ELSE 0 END) AS enterprise_monthly_fee,
    SUM(CASE WHEN segment = 'Consumer' THEN monthly_fee ELSE 0 END) AS consumer_monthly_fee
FROM reconciled
GROUP BY ROLLUP (DATE_TRUNC('MONTH', start_date_month_end))
HAVING event_count > 1
ORDER BY event_count DESC NULLS FIRST;
