select signup_date as event_date from customers where signup_date < '2023-09-01' union select shipped_date as event_date from shipments where status = 'RETURNED'
