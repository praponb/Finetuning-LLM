select id, age, is_active from customers where age = (select min(age) from customers);
