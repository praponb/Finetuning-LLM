SELECT s.* FROM accounts s WHERE NOT EXISTS (SELECT 1 FROM transactions x WHERE x.account_id = s.id AND x.merchant NOT IN ('Amazon', '7-Eleven'));
