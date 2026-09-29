select s.id, s.start_date, c.age from subscriptions s inner join customers c on c.id = s.customer_id order by c.age
