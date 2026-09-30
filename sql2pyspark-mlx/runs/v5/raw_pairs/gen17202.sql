select sale_date as event_date from store_sales intersect select signup_date as event_date from customers where signup_date <= '2024-08-01';
