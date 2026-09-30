SELECT id, signup_date, country, city FROM customers WHERE segment IN ('Consumer', 'SMB') AND segment IN ('Consumer', 'Corporate');
