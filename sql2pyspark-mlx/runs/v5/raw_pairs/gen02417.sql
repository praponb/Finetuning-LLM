SELECT name, CASE WHEN country IN ('SG', 'US') THEN 'Priority' ELSE 'Other' END AS country_group FROM customers WHERE country <> 'DE' AND email LIKE '%@mail.co.th' ORDER BY name DESC;
