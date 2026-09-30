SELECT id, balance, LAST_DAY(opened_date) AS opened_date_month_end FROM accounts WHERE opened_date < '2024-07-01' OR status IS NOT NULL;
