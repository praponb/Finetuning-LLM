SELECT id, account_type, opened_date, WEEKOFYEAR(opened_date) AS opened_date_weekofyear FROM accounts ORDER BY opened_date_weekofyear DESC
