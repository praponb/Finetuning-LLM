SELECT id, city, opened_date, region FROM stores WHERE (sqft >= 8875 AND city = 'New York') OR city IN ('New York', 'London', 'Berlin') ORDER BY id DESC, city DESC
