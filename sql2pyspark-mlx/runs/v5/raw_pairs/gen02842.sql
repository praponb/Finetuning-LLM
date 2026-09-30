select c.city, count(*) as row_count from orders o inner join customers c on o.customer_id = c.id group by c.city order by city desc;
