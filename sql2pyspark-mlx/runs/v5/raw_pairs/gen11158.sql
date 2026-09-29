WITH matched_keys AS (
    SELECT plan, customer_id
    FROM subscriptions
    EXCEPT DISTINCT
    SELECT plan, customer_id
    FROM subscriptions
    WHERE plan = 'standard'
),
reconciled AS (
    SELECT s.*
    FROM subscriptions s
    JOIN matched_keys k ON s.plan = k.plan AND s.customer_id = k.customer_id
)
SELECT
    id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status
FROM reconciled
ORDER BY status DESC;
