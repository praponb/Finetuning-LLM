select c.city, max(o.discount) as max_discount, sum(case when o.channel = 'partner' then 1 else 0 end) as partner_count from orders o join customers c on o.customer_id = c.id group by c.city;
