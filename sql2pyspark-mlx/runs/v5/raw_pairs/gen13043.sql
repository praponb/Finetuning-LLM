SELECT
    balance,
    opened_date,
    customer_id,
    CASE WHEN balance >= 72000 THEN 'High' WHEN balance >= 29000 THEN 'Medium' ELSE 'Low' END AS balance_tier
FROM accounts
