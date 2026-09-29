SELECT plan, YEAR(start_date) AS start_year, SUM(CASE WHEN plan = 'basic' THEN monthly_fee ELSE 0 END) AS basic_monthly_fee FROM subscriptions GROUP BY 1, 2
