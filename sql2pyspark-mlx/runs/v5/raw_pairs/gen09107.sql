SELECT CASE WHEN account_type IN ('CREDIT', 'CHECKING') THEN 'group_a' ELSE 'other' END AS account_type_group, CASE WHEN balance >= 67000 THEN 'high' WHEN balance >= 37000 THEN 'mid' ELSE 'low' END AS balance_bucket, COUNT(*) AS account_count
FROM accounts
WHERE balance > 77000
GROUP BY account_type_group, balance_bucket;
