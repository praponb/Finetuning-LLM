SELECT age, segment, ADD_MONTHS(signup_date, 1) signup_date_plus_months, CASE WHEN is_active THEN 'Yes' ELSE 'No' END is_active_label FROM customers WHERE age = 39 ORDER BY segment DESC
