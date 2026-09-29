SELECT segment, age FROM customers WHERE NOT is_active UNION ALL SELECT segment, age FROM customers WHERE segment IN ('SMB', 'Consumer', 'Corporate');
