SELECT id,
       CASE WHEN segment = 'Corporate' AND is_active = TRUE THEN 0.20
            WHEN segment = 'SMB' THEN 0.10
            ELSE 0.0 END AS discount_rate
FROM customers;
