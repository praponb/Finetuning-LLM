select s.id, s.plan, s.end_date, c.city, c.country, case when s.status = 'EXPIRED' then 1 else 0 end as is_expired from subscriptions s left join customers c on s.customer_id = c.id
