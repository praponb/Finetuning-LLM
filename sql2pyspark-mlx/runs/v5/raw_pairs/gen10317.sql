select a.id, a.status, c.segment, c.age from accounts a left outer join customers c on a.customer_id = c.id order by id limit 10
