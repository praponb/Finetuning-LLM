select c.id, c.country, c.age, c.email from customers c where c.age = (select min(c2.age) from customers c2 where c2.country = c.country);
