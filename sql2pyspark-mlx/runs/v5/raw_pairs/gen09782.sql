SELECT sqft, city, opened_date FROM stores WHERE region IN ('West', 'East', 'South') AND opened_date >= '2024-12-01';
