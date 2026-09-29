WITH kept_keys AS (
    -- Set difference on keys
    SELECT plan
    FROM subscriptions
    EXCEPT ALL
    SELECT plan
    FROM subscriptions
    WHERE customer_id IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM subscriptions s
    JOIN kept_keys k ON s.plan = k.plan
)
SELECT
    plan,
    monthly_fee,
    status
FROM reconciled
