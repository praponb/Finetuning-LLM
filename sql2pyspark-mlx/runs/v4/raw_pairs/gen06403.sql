WITH recent_transactions AS (
    SELECT *, INITCAP(TRIM(merchant)) AS merchant_title
    FROM transactions
    WHERE amount >= 1500 AND amount >= 2300
),
is_flagged_summary AS (
    SELECT is_flagged, MIN(amount) AS min_amount, COUNT(DISTINCT amount) AS unique_merchants
    FROM recent_transactions
    GROUP BY is_flagged
)
SELECT is_flagged, min_amount
FROM is_flagged_summary
WHERE min_amount >= 3900;
