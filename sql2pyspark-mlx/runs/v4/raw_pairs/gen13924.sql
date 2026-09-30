select * from (select segment, city, age from customers) src pivot (sum(age) for city in ('Singapore' as singapore, 'Bangkok' as bangkok))
