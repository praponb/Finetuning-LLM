WITH remaining_keys AS (
    SELECT plan, status
    FROM subscriptions
    EXCEPT ALL
    SELECT plan, status
    FROM subscriptions
    WHERE monthly_fee BETWEEN 200 AND 210
),
reconciled AS (
    SELECT s.*
    FROM subscriptions s
    INNER JOIN remaining_keys k ON s.plan = k.plan AND s.status = k.status
),
enriched AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan, status ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_percent_rank,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 4) AS monthly_fee_share
    FROM reconciled
),
kept_keys AS (
    SELECT status, plan
    FROM enriched
    EXCEPT ALL
    SELECT status, plan
    FROM subscriptions
    WHERE status = 'EXPIRED'
),
verified AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.status = k.status AND s.plan = k.plan
),
enriched_subscriptions AS (
    SELECT
        src.id,
        src.customer_id,
        src.plan,
        src.monthly_fee,
        src.status,
        src.monthly_fee_percent_rank,
        p.name
    FROM verified src
    LEFT JOIN customers p
        ON src.customer_id <=> p.id
),
with_dims AS (
    SELECT
        s.id,
        s.customer_id,
        s.monthly_fee,
        s.monthly_fee_percent_rank,
        b.age,
        b.city
    FROM enriched_subscriptions s
    INNER JOIN customers b
        ON s.customer_id <=> b.id
    WHERE b.signup_date BETWEEN '2023-06-01' AND '2024-03-01'
)
SELECT
    id,
    customer_id,
    monthly_fee,
    monthly_fee_percent_rank,
    age
FROM with_dims;
