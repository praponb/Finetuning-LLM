WITH recent_accounts AS (
    SELECT *, DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_date_label
    FROM accounts
    WHERE account_type != 'CHECKING' AND balance BETWEEN 28000 AND 58000 AND customer_id IN (25, 26)
),
summary AS (
    SELECT CASE WHEN balance >= 61000 THEN 'high' WHEN balance >= 27000 THEN 'mid' ELSE 'low' END AS balance_bucket, QUARTER(opened_date) AS opened_quarter, MAX(opened_date) AS last_opened_date
    FROM recent_accounts
    GROUP BY CASE WHEN balance >= 61000 THEN 'high' WHEN balance >= 27000 THEN 'mid' ELSE 'low' END, QUARTER(opened_date)
)
SELECT balance_bucket, opened_quarter, last_opened_date
FROM summary
ORDER BY 3 DESC;
