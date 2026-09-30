SELECT store_name, opened_date FROM stores WHERE sqft BETWEEN 5493 AND 8641 UNION SELECT store_name, opened_date FROM stores WHERE opened_date >= '2023-09-01';
