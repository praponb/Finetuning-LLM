WITH enriched AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) AS credit_limit_bucket,
        LEAD(credit_limit, 1) OVER (PARTITION BY city ORDER BY age, id) AS next_credit_limit
    FROM customers
    WHERE credit_limit IS NULL
)
SELECT
    id,
    name,
    email,
    country,
    segment,
    is_active,
    credit_limit,
    age,
    credit_limit_bucket
FROM enriched;
