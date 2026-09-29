WITH recent_accounts AS (
    SELECT *, CAST(balance AS INT) AS balance_int
    FROM accounts
    WHERE balance >= 60000 AND opened_date < '2023-06-17' AND balance <= 68000
),
agg AS (
    SELECT
        YEAR(opened_date) AS opened_year,
        customer_id,
        COUNT(balance) AS balance_count,
        COUNT(DISTINCT balance_int) AS unique_statuss
    FROM recent_accounts
    GROUP BY YEAR(opened_date), customer_id
)
SELECT
    opened_year,
    customer_id,
    balance_count,
    unique_statuss
FROM agg
