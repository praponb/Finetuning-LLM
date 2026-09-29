select c.is_active, count(distinct s.monthly_fee) as unique_statuss from subscriptions s left join customers c on s.customer_id = c.id group by c.is_active;
