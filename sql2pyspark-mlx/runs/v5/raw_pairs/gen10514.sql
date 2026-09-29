WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        LENGTH(status) AS status_length
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
base_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT(status, ' ', plan) AS status_plan
    FROM base
    WHERE end_date >= TO_DATE('2023-05-08')
      AND end_date > '2023-11-01'
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.credit_limit,
        p.signup_date
    FROM base_subscriptions src
    LEFT JOIN customers p
        ON src.customer_id = p.id
    WHERE p.segment IN ('Corporate', 'SMB', 'Consumer')
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY status_length DESC NULLS LAST, id) = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        customer_id,
        credit_limit,
        monthly_fee
    FROM deduped
) src
UNPIVOT (
    value FOR measure IN (credit_limit AS CREDIT_LIMIT_VAL, monthly_fee)
) u;
