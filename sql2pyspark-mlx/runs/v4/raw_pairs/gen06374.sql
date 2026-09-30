WITH src AS (
    SELECT *
    FROM accounts
    WHERE status = 'FROZEN' AND balance BETWEEN 49000 AND 71000 AND account_type NOT IN ('CHECKING', 'SAVINGS', 'LOAN')
),
summary AS (
    SELECT DATE_FORMAT(opened_date, 'yyyy-MM') opened_month, COUNT(balance) balance_count
    FROM src
    GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
)
SELECT opened_month, balance_count
FROM summary
WHERE balance_count >= 7
