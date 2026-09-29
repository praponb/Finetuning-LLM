select c.segment, min(s.started_at) as min_started_at from sessions s full outer join customers c on s.customer_id = c.id group by c.segment
