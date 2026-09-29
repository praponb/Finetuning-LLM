SELECT is_active, MIN(signup_date) AS first_signup_date, SORT_ARRAY(COLLECT_SET(country)) AS country_list FROM customers GROUP BY is_active;
