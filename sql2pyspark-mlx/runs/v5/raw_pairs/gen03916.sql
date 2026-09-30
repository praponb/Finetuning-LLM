SELECT plan, COUNT(*) AS row_count, SUM(CASE WHEN status = 'CANCELLED' THEN monthly_fee ELSE 0 END) AS cancelled_monthly_fee FROM subscriptions GROUP BY plan
