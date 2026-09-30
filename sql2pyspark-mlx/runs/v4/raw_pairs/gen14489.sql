SELECT *, CASE WHEN region IN ('East', 'South') THEN 'Core' ELSE 'Other' END AS region_group FROM stores WHERE sqft = 15741
