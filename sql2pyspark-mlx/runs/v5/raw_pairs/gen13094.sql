SELECT id, name, credit_limit - age AS credit_limit_minus_age FROM customers WHERE age > 50 AND is_active AND segment NOT IN ('Consumer', 'SMB')
