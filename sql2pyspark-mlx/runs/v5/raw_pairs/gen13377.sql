WITH enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.country,
        p.credit_limit,
        p.signup_date
    FROM subscriptions e
    LEFT JOIN customers p
        ON e.customer_id <=> p.id
    WHERE p.segment IN ('SMB', 'Corporate', 'Consumer')
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.email,
        b.name,
        b.country AS customer_country
    FROM enriched a
    LEFT JOIN customers b
        ON a.customer_id <=> b.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS seq_no,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY monthly_fee NULLS LAST) AS monthly_fee_percent_rank,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY customer_country, plan), 4) AS credit_limit_share
    FROM enriched_subscriptions
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        customer_id,
        plan,
        status,
        credit_limit,
        name,
        customer_country,
        monthly_fee_percent_rank
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC NULLS LAST, id) = 1
)
SELECT
    id,
    status,
    name
FROM deduped
ORDER BY id, status DESC
