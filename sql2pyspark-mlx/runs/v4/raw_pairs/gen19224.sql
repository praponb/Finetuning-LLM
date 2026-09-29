SELECT id, segment, age, credit_limit, CAST(age AS STRING) AS age_str FROM customers WHERE country = 'US' AND segment = 'Corporate' AND segment IN ('SMB', 'Consumer', 'Corporate')
