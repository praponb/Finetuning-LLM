select c.name, c.age, o.order_date, o.status, concat_ws('-', c.segment, c.city) as segment_city_key from customers c left join orders o on o.customer_id = c.id where c.age <= 39 order by c.name desc;
