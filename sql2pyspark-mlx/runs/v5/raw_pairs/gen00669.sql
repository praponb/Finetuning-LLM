select s.status, c.segment from subscriptions s left outer join customers c on s.customer_id = c.id where s.customer_id is not null order by 2, status
