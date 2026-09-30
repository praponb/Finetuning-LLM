select c.city, count(*) as cnt from sessions s join customers c on s.customer_id = c.id where s.country = 'UK' group by c.city order by c.city
