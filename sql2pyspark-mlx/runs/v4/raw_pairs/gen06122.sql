WITH src AS (
    SELECT *, LOWER(merchant) AS merchant_lower
    FROM transactions
    WHERE account_id IN (11, 12, 13, 37) AND txn_type = 'FEE' AND amount BETWEEN 3600 AND 3800
),
Agg AS (
    SELECT txn_type, COUNT(amount) AS amount_count, SUM(CASE WHEN merchant = '7-Eleven' THEN amount ELSE 0 END) AS 7_eleven_amount
    FROM src
    GROUP BY txn_type
)
SELECT txn_type, amount_count, 7_eleven_amount
FROM Agg
WHERE amount_count > 8;
