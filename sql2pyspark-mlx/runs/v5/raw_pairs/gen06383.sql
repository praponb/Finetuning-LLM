select c.is_active, sum(case when s.country = 'DE' then 1 else 0 end) as de_count from sessions s left outer join customers c on s.customer_id = c.id group by c.is_active order by de_count;
