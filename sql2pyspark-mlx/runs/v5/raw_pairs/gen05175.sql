SELECT *, DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label, CASE WHEN sqft >= 15784 THEN 'High' WHEN sqft >= 5788 THEN 'Medium' ELSE 'Low' END AS sqft_tier FROM stores;
