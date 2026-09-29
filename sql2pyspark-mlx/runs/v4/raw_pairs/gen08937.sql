SELECT signup_date FROM customers WHERE segment IN ('SMB', 'Consumer', 'Corporate') UNION SELECT shipped_date FROM shipments ORDER BY signup_date
