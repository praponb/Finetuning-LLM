SELECT status, 'account' AS source FROM accounts UNION ALL SELECT status, 'payment' AS source FROM payments WHERE method NOT IN ('card', 'cash') ORDER BY source;
