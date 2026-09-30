select c.id, c.country, c.age, c.credit_limit from customers c where c.age = (select max(c2.age) from customers c2 where c2.country = c.country) order by c.credit_limit desc, c.country;
