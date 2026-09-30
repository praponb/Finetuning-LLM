WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DATE_ADD(opened_date, 89) opened_date_plus_89d,
        DATEDIFF(CURRENT_DATE(), opened_date) days_since_opened_date,
        COALESCE(status, 'UNKNOWN') resolved_status
    FROM accounts
    WHERE opened_date BETWEEN '2023-06-30' AND '2024-05-01'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time >= s.opened_date_plus_89d
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.city,
        c.age
    FROM not_flagged e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END is_berlin,
        SUBSTRING(city, 1, 3) city_prefix
    FROM enriched
    WHERE age = 48
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        status,
        customer_id,
        account_type,
        balance
    FROM filtered
) src
PIVOT (
    AVG(balance)
    FOR account_type IN ('SAVINGS', 'CREDIT', 'LOAN')
);
