SELECT s.* FROM customers s WHERE NOT EXISTS (SELECT 1 FROM accounts x WHERE x.customer_id = s.id AND x.account_type IN ('CHECKING', 'LOAN', 'CREDIT'));
