select s.id, s.customer_id, c.is_active from subscriptions s left join customers c on c.id = s.customer_id order by s.customer_id desc;
