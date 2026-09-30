select c.id, c.country, c.age, c.is_active from customers c where c.age < (select avg(c2.age) from customers c2 where c2.country = c.country) order by c.age;
