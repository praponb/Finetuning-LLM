WITH ValidCustomers AS (
    SELECT *
    FROM customers
    WHERE segment IN ('SMB', 'Corporate') AND credit_limit BETWEEN 6200 AND 13000
),
customers_stats AS (
    SELECT DATE_TRUNC('MONTH', signup_date) AS signup_month, COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count
    FROM ValidCustomers
    GROUP BY DATE_TRUNC('MONTH', signup_date)
)
SELECT signup_month, sg_count
FROM customers_stats
WHERE sg_count < 9;
