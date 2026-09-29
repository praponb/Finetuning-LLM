SELECT opened_date FROM stores WHERE sqft BETWEEN 13375 AND 15667 UNION SELECT opened_date FROM stores WHERE city IN ('Bangkok', 'Berlin', 'London') ORDER BY opened_date;
