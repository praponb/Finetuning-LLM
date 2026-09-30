WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DATE_ADD(opened_date, 47) AS opened_date_plus_47d
    FROM accounts
    WHERE opened_date > DATE '2023-06-22'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.credit_limit,
        p.country
    FROM cleaned src
    JOIN customers p
        ON src.customer_id = p.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, country ORDER BY credit_limit NULLS FIRST, id) = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        status,
        country,
        credit_limit
    FROM deduped
) src
PIVOT (
    AVG(credit_limit)
    FOR country IN ('UK', 'JP', 'DE')
);
