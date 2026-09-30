select status, plan from subscriptions except distinct select status, plan from subscriptions where year(end_date) = 2025;
