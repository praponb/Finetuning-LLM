WITH enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.credit_limit,
        c.age
    FROM accounts s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type, customer_id ORDER BY age, id) = 1
),
enriched_accounts AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.segment,
        c.credit_limit AS customer_credit_limit,
        c.email
    FROM latest e
    LEFT JOIN customers c
        ON e.customer_id = c.id
)
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    credit_limit,
    age,
    segment,
    email
FROM enriched_accounts
ORDER BY id DESC;
