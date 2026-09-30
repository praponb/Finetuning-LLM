WITH cleaned AS (
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        CAST(monthly_fee AS INT) AS monthly_fee_int
    FROM subscriptions
    WHERE customer_id IS NULL
),
enriched AS (
    SELECT
        s.*,
        b.credit_limit,
        b.segment
    FROM cleaned s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
enriched_subscriptions AS (
    SELECT src.*, d.name
    FROM enriched src
    INNER JOIN customers d
        ON src.customer_id = d.id
)
SELECT
    id,
    start_date,
    monthly_fee,
    status,
    credit_limit,
    segment,
    name
FROM enriched_subscriptions
ORDER BY credit_limit DESC NULLS LAST;
