SELECT id, email, is_active, CASE WHEN country IN ('JP', 'DE') THEN 'Group A' ELSE 'Other' END AS country_group FROM customers;
