SELECT store_name, opened_date, DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date, INITCAP(TRIM(region)) AS region_title FROM stores ORDER BY region_title DESC
