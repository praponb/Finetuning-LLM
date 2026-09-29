SELECT merchant FROM transactions WHERE account_id IN (1, 40) UNION SELECT merchant FROM transactions WHERE merchant NOT IN ('7-Eleven', 'Shell', 'Amazon');
