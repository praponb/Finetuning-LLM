select * from (select is_active, city, age from customers)
pivot (sum(age) for city in ('Tokyo', 'Bangkok'))
