SELECT *, DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date FROM accounts ORDER BY id DESC;
