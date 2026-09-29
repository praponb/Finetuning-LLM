WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        signup_date,
        is_active,
        credit_limit,
        age,
        ROUND(credit_limit * 1.07, 2) AS credit_limit_scaled,
        DATE_ADD(signup_date, 4) AS signup_date_plus_4d
    FROM customers
    WHERE signup_date >= DATE '2023-07-07'
      AND credit_limit >= 8800
)
-- Keep one row per key (latest first)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit DESC NULLS FIRST, id) AS latest_rank
    FROM filtered
) ranked
WHERE latest_rank = 1;
