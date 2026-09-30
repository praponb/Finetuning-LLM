SELECT id, account_id, CASE WHEN merchant = 'Starbucks' THEN 1 ELSE 0 END AS is_starbucks FROM transactions ORDER BY id DESC;
