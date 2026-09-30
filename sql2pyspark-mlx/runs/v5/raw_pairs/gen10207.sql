SELECT is_active, segment, ROUND(AVG(credit_limit), 2) avg_credit_limit FROM customers WHERE country = 'TH' GROUP BY ROLLUP(is_active, segment)
