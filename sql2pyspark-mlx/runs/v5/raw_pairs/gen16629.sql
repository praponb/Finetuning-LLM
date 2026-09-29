SELECT is_flagged, COALESCE(merchant, 'N/A') AS merchant_filled
FROM transactions
WHERE txn_time < '2023-10-01' AND amount <= 3900
