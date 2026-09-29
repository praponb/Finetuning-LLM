SELECT id, region, opened_date, DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date FROM stores ORDER BY id DESC, opened_date LIMIT 5;
