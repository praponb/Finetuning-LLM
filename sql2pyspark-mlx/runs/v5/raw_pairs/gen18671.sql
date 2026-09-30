select s.id, s.plan, s.status, c.age, c.segment, c.is_active from subscriptions s left join customers c on s.customer_id = c.id where c.age = 31 order by age desc;
