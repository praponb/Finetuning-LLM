SELECT opened_date FROM accounts WHERE opened_date > '2025-08-01' INTERSECT SELECT hire_date FROM employees ORDER BY opened_date DESC
