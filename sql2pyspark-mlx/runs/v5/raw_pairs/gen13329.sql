SELECT opened_date FROM stores WHERE city = 'Berlin' EXCEPT SELECT end_date AS opened_date FROM subscriptions WHERE end_date < '2024-04-02'
