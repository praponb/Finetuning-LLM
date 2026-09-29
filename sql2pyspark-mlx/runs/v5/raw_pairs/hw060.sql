SELECT id, CASE WHEN country IN ('US', 'UK', 'DE') THEN 'West' ELSE 'Asia' END AS market FROM customers;
