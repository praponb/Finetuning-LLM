WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        status,
        LAST_DAY(opened_date) AS opened_date_month_end
    FROM accounts
    WHERE opened_date > DATE '2023-11-07'
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.city,
        b.signup_date
    FROM base a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
joined_accounts AS (
    -- Attach customers attributes
    SELECT a.*, c.email
    FROM joined a
    LEFT JOIN customers c
        ON a.customer_id = c.id
)
SELECT
    id,
    opened_date,
    status,
    city,
    signup_date,
    email,
    CASE WHEN city RLIKE '^(Berlin|Singapore)$' THEN 'GROUP_A' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'CORE' ELSE 'EXTENDED' END AS city_zone
FROM joined_accounts
ORDER BY city_zone;
