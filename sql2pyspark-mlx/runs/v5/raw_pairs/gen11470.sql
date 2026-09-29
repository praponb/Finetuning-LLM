SELECT CASE WHEN merchant IN ('Grab', 'Shell') THEN 'tier_1' ELSE 'other' END AS merchant_group, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM transactions
WHERE amount >= 2000 AND txn_type IN ('FEE', 'CREDIT', 'REFUND')
GROUP BY 1
ORDER BY stddev_amount ASC;
