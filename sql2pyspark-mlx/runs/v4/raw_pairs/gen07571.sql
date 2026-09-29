select s.id, s.start_date, s.plan, c.credit_limit, c.is_active from subscriptions s left outer join customers c on c.id = s.customer_id where c.city = 'New York' order by id desc
