select customer_id, datediff(current_date(), opened_date) as days_since_opened_date from accounts where opened_date < '2024-12-01'
